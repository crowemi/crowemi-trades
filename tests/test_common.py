import os

from crowemi_trades.storage.mongodb_storage import MongoDbStorage


def setUp():
    pass


def configure_mongodb() -> MongoDbStorage:
    return MongoDbStorage(os.getenv("MONGODB_URI"))
