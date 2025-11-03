import logging
import pickle
import traceback
from typing import Any, Optional
from datetime import datetime, timezone

import pymongo
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError

from .base import SessionStore
from ..base import Session
from ..config import AKConfig

class MongoDriver:
    """
    MongoDriver provides MongoDB connection and helper methods for session document operations.
    """
    _mongo_client = None
    _mongo_db = None
    _mongo_collection = None

    def __init__(self):
        self._log = logging.getLogger("ak.core.sessions.mongo.util")
        self._url = AKConfig.get().session.mongo.url
        self._database_name = AKConfig.get().session.mongo.database
        self._collection_name = AKConfig.get().session.mongo.collection
        self._ttl = int(AKConfig.get().session.mongo.ttl)

    @property
    def collection(self) -> pymongo.collection.Collection:
        """
        Returns the MongoDB collection instance.
        """
        if self._mongo_collection is None:
            self._connect()
        return self._mongo_collection

    @property
    def ttl(self) -> int:
        """
        Returns the configured TTL for MongoDB documents.
        """
        return self._ttl

    def _connect(self):
        """
        Connects to MongoDB using the configured URL and sets up the DB and collection.
        """
        try:
            self._log.debug(f"Connecting to MongoDB at {self._url}")
            # serverSelectionTimeoutMS mimics the connect_timeout
            client = pymongo.MongoClient(
                self._url,
                serverSelectionTimeoutMS=5000
            )
            # Ping the server to confirm connection
            client.admin.command('ping')
            self._log.debug("MongoDB connection successful")

            self._mongo_client = client
            self._mongo_db = self._mongo_client[self._database_name]
            self._mongo_collection = self._mongo_db[self._collection_name]

            self._ensure_ttl_index()

        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            self._log.error(f"Failed to connect to MongoDB: {e}")
            self._log.error(traceback.format_exc())
            raise

    def _ensure_ttl_index(self):
        """
        Ensures the TTL index exists on the `last_updated` field.
        If the index exists with a different TTL, it will be recreated.
        """
        if self._ttl <= 0:
            self._log.debug("TTL is 0, skipping TTL index creation.")
            return

        index_name = "session_ttl_index"
        indexes = self.collection.index_information()

        if index_name in indexes:
            existing_ttl = indexes[index_name].get('expireAfterSeconds')
            if existing_ttl == self._ttl:
                self._log.debug(f"TTL index '{index_name}' already exists with correct TTL.")
                return
            else:
                self._log.warning(
                    f"TTL index '{index_name}' has incorrect TTL ({existing_ttl}s). "
                    f"Dropping and recreating with {self._ttl}s."
                )
                self.collection.drop_index(index_name)

        self._log.info(f"Creating TTL index '{index_name}' on 'last_updated' field with {self._ttl}s expiry.")
        self.collection.create_index(
            "last_updated",
            name=index_name,
            expireAfterSeconds=self._ttl
        )

    def get_document(self, session_id: str) -> Optional[dict]:
        """
        Retrieves the entire session document for a given session ID.
        :param session_id: The session ID (document _id).
        :return: The session document, or None if not found.
        """
        self._log.debug(f"GET document {session_id}")
        return self.collection.find_one({"_id": session_id})

    def replace(self, session_id: str, document: dict) -> None:
        """
        Replaces a session document, or inserts it if it doesn't exist (upsert).
        Automatically adds/updates the `last_updated` timestamp.
        :param session_id: The session ID (document _id).
        :param document: The full document to replace the old one with.
        """
        document["last_updated"] = datetime.now(timezone.utc)
        self._log.debug(f"REPLACE document {session_id}")
        # We must add _id to the document itself for upsert=True to work
        document["_id"] = session_id
        self.collection.replace_one(
            {"_id": session_id},
            document,
            upsert=True
        )

    def exists(self, session_id: str) -> bool:
        """
        Checks if a session document exists.
        :param session_id: The key to check.
        :return: True if the key exists, False otherwise.
        """
        try:
            return self.collection.count_documents({"_id": session_id}) > 0
        except pymongo.errors.PyMongoError:
            return False

    def clear_all(self) -> None:
        """
        Clears all documents from the session collection.
        """
        self._log.info(f"Clearing all sessions from collection {self._collection_name}")
        self.collection.delete_many({})

class MongoSessionStore(SessionStore):
    """
    MongoSessionStore class provides a MongoDB-based implementation of the SessionStore interface.
    """

    def __init__(
            self,
            driver: MongoDriver
    ):
        """
        Initializes a MongoSessionStore instance.
        :param driver: MongoDriver instance
        """
        self._log = logging.getLogger("ak.core.sessions.mongo")
        self._serde = MongoSessionSerde()  # Using the pickle-based serde
        self._driver = driver

    def load(self, session_id: str, strict: bool = False) -> Session:
        """
        Loads a session by its unique identifier.
        :param session_id: Unique identifier for the session.
        :param strict: If True, raises an exception if the session is not found.
        :return: The session associated with the identifier, or a new session if it does not exist.
        """
        self._log.debug(f"Loading mongo session with ID {session_id}")

        document = self._driver.get_document(session_id)

        if document:
            session = Session(session_id)
            for field, value in document.items():
                if field in ["_id", "last_updated", "__init__"]:
                    continue
                session.set(field, self._serde.loads(value))
            return session
        else:
            if strict:
                raise KeyError(f"Session {session_id} not found")
            self._log.warning(f"Session {session_id} not found, creating new session")
            return self.new(session_id)

    def new(self, session_id: str) -> Session:
        """
        Initialize a session for a given session id.
        :param session_id: Unique identifier for the session.
        :return: The session associated with the identifier.
        """
        self._log.debug(f"Creating new mongo session with ID {session_id} ")

        # Create a minimal document so the key exists and TTL can apply
        minimal_doc = {
            "__init__": self._serde.dumps(True)
        }
        self._driver.replace(session_id, minimal_doc)

        return Session(session_id)

    def clear(self) -> None:
        """
        Clears all stored sessions for this store's collection.
        """
        self._driver.clear_all()

    def store(self, session: Session) -> None:
        """
        Stores a session or updates it if it already exists in the storage.
        This performs a full replacement of the document.
        :param session: The session to store.
        """
        self._log.debug(f"Storing session {session.id}")

        # Build the document from all keys in the session
        document_to_store = {}
        keys = list(session.get_all_keys())

        for key in keys:
            value = session.get(key)
            document_to_store[key] = self._serde.dumps(value)

        # If the session is empty (e.g., all keys removed),
        # we still store it to update the TTL.
        # We add __init__ to ensure it's not a truly empty doc.
        if not keys:
            document_to_store["__init__"] = self._serde.dumps(True)

        self._driver.replace(session.id, document_to_store)

class MongoSessionSerde:
    """
    MongoSessionSerde provides serialization and deserialization of Session objects for
    MongoDB storage using pickle.
    """
    _log = logging.getLogger("ak.core.sessions.mongoserde")

    @classmethod
    def dumps(cls, obj: Any) -> bytes:
        """
        Serialize an object using pickle.
        :param obj: The value to serialize.
        :return: The serialized value as bytes.
        """
        cls._log.debug(f"dumped: {obj}")
        return pickle.dumps(obj)

    @classmethod
    def loads(cls, payload: bytes) -> Any:
        """
        Deserialize bytes back into a Python object.
        :param payload: The bytes to deserialize (BSON Binary type).
        :return: The deserialized value.
        """
        cls._log.debug(f"loads: {type(payload)}")
        if payload is None:
            return None
        loaded = pickle.loads(payload)
        cls._log.debug(f"loaded: {loaded}")
        return loaded
