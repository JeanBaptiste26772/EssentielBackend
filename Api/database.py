import os
import certifi
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
from pathlib import Path

env_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=env_path, override=True)

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB  = os.getenv("MONGO_DB", "burkina_news")
MONGO_COL = "articles"

client: AsyncIOMotorClient = None
db = None

async def connect_db():
    global client, db
    client = AsyncIOMotorClient(
        MONGO_URI,
        serverSelectionTimeoutMS=5000,
        tlsCAFile=certifi.where()  # ← UN SEUL client, avec certifi
    )
    db = client[MONGO_DB]
    print(f"✅ API connectée à MongoDB — base : {MONGO_DB}, collection : {MONGO_COL}")

async def close_db():
    global client
    if client:
        client.close()
        print("🔌 Déconnecté de MongoDB")

def get_db():
    return db