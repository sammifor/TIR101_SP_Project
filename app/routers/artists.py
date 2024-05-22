from typing import Optional
from fastapi import FastAPI, APIRouter, HTTPException, status
from app.logics.artist_logics import get_artists_all, get_artists_detail
from google.cloud import bigquery
from dags.utils.GCP_client import get_bq_client

app = FastAPI()  # 建立一個 FastAPI application

# 創建一個新的路由器（Router）用於處理 /artists 路徑
artists_router = APIRouter()


# @artists_router.get("/test_bigquery_connection")
# def test_bigquery_connection():
#     try:
#         client = get_bq_client()
#         datasets = list(client.list_datasets())
#         if datasets:
#             return {"message": "Connection to BigQuery successful"}
#         else:
#             return {"message": "No datasets found in BigQuery"}
#     except Exception as e:
#         return {"error": str(e)}


@artists_router.get(
    "/list_all_artist",
    operation_id="list_all_artists",
    tags=["Artists"],
    summary="List All Artists",
    description="List all Artists data",
    response_description="All Artist data in BigQuery",
    status_code=status.HTTP_200_OK,
)
def list_all_artists():
    try:
        result = get_artists_all()
        if not result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="No artists found"
            )
        return result
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


@artists_router.get(
    "/{artist_id}",
    tags=["Artists"],
    operation_id="list_artists_detail",
    summary="Get Specific Artists",
    description="Get data of input Artist id",
    response_description="Specific Artist id data",
    status_code=status.HTTP_200_OK,
)
def list_artists_detail(artist_id: str):
    try:
        result = get_artists_detail(artist_id)
        if not result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="No artists found"
            )
        return result
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


# 將 tracks_router 掛載到應用程式中，指定 /tracks 為前綴
app.include_router(artists_router, prefix="/artists")

# 下面是原本的根路徑和用戶路徑
# @app.get("/", tags=["Root"])  # 指定 api 路徑 (get方法)，並指定 tag 為 "Root"
# def read_root():
#     return {"Hello": "World"}


# @app.get("/users/{user_id}", tags=["Users"])  # 指定 api 路徑 (get方法)，並指定 tag 為 "Users"
# def read_user(user_id: int, q: Optional[str] = None):
#     return {"user_id": user_id, "q": q}
