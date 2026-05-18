from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from Api.routes import articles, audio
from Api.database import connect_db, close_db

@asynccontextmanager
async def lifespan(app: FastAPI):
    await connect_db()
    yield
    await close_db()

app = FastAPI(
    title="Portail Actualité Burkina API",
    description="API de lecture des articles résumés et traduits en mooré",
    version="1.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["X-Total-Count"], 
)

app.include_router(articles.router, prefix="/articles", tags=["Articles"])
app.include_router(audio.router, prefix="/audio", tags=["Audio"])

@app.get("/")
async def root():
    return {"message": "Portail Actualité Burkina — API opérationnelle"}