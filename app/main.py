from fastapi import FastAPI
from app.routers.artists import artists_router
from app.routers.tracks import tracks_router

app = FastAPI()
#uvicorn app.main:app --reload

app.include_router(artists_router, prefix="/artists")
app.include_router(tracks_router, prefix="/tracks")

