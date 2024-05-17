from typing import Optional

from fastapi import FastAPI, APIRouter

app = FastAPI()  # 建立一個 FastAPI application
# 確保你在terminal 的路徑是在 /app folder底下
#uvicorn app:app --reload

# 創建一個新的路由器（Router）用於處理 /tracks 路徑
tracks_router = APIRouter()


@tracks_router.get("/get_tracks", tags=["Tracks"])  # 在 tracks 路由器中定義 /get_tracks 路徑，並指定 tag 為 "Tracks"
def get_tracks():
    return {"message": "Get specific tracks"}


@tracks_router.get("/list_all_tracks", tags=["Tracks"])  # 在 tracks 路由器中定義 /list_all_tracks 路徑，並指定 tag 為 "Tracks"
def list_all_tracks():

    return {"message": "List all tracks"}


# 將 tracks_router 掛載到應用程式中，指定 /tracks 為前綴
app.include_router(tracks_router, prefix="/tracks")


# 下面是原本的根路徑和用戶路徑
@app.get("/", tags=["Root"])  # 指定 api 路徑 (get方法)，並指定 tag 為 "Root"
def read_root():
    return {"Hello": "World"}


@app.get("/users/{user_id}", tags=["Users"])  # 指定 api 路徑 (get方法)，並指定 tag 為 "Users"
def read_user(user_id: int, q: Optional[str] = None):
    return {"user_id": user_id, "q": q}

