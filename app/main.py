from fastapi import FastAPI
from app.routers.artists import artists_router
from app.routers.tracks import tracks_router
from app.routers.charts import charts_router

app = FastAPI()
# uvicorn app.main:app --reload

app.include_router(charts_router, prefix="/charts")
app.include_router(artists_router, prefix="/artists")
app.include_router(tracks_router, prefix="/tracks")


# 下面是原本的根路徑和用戶路徑
# @app.get("/", tags=["Root"])  # 指定 api 路徑 (get方法)，並指定 tag 為 "Root"
# def read_root():
#     return {"Hello": "World"}


# @app.get("/users/{user_id}", tags=["Users"])  # 指定 api 路徑 (get方法)，並指定 tag 為 "Users"
# def read_user(user_id: int, q: Optional[str] = None):
#     return {"user_id": user_id, "q": q}
