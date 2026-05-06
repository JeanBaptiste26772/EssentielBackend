import os
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB  = os.getenv("MONGO_DB", "burkina_news")   # ← même valeur que le scraper
MONGO_COL = "articles"                               # ← même collection que le scraper

client: AsyncIOMotorClient = None
db = None

async def connect_db():
    global client, db
    client = AsyncIOMotorClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    db = client[MONGO_DB]
    print(f"✅ API connectée à MongoDB — base : {MONGO_DB}, collection : {MONGO_COL}")

async def close_db():
    global client
    if client:
        client.close()
        print("🔌 Déconnecté de MongoDB")

def get_db():
    return db