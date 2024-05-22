from typing import Optional
from fastapi import FastAPI, APIRouter, HTTPException, status
from app.logics.artist_logics import (
    get_artists_all,
    get_artists_detail,
    get_artists_number,
)

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
    "/count_artists",
    tags=["Artists"],
    operation_id="count_artists",
    summary="The number of Artists",
    description="Get the number of Artists",
    response_description="The number of Artists",
    status_code=status.HTTP_200_OK,
)
def count_artists():
    try:
        result = get_artists_number()
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
    "/all_artists",
    tags=["Artists"],
    operation_id="all_artists",
    summary="All Artists List",
    response_description="All Artists data",
    status_code=status.HTTP_200_OK,
)
def all_artists():
    """
    Get all Artists data
    - **trackMetadata_artists_spotifyUri**: artist id
    - **genres_id**: genre id
    - **artist_name**: artist name
    - **artist_followers**: artist followers
    - **artist_popularity**: artist popularity

    FROM: Schema_Use_Final.dim_Artist
    """
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
    operation_id="artists_detail",
    summary="Get Specific Artists",
    response_description="Specific Artist id data",
    status_code=status.HTTP_200_OK,
)
def artists_detail(artist_id: str):
    """
    Get data of input Artist id
    - **trackMetadata_artists_spotifyUri**: artist id
    - **genres_id**: genre id
    - **artist_name**: artist name
    - **artist_followers**: artist followers
    - **artist_popularity**: artist popularity

    FROM: Schema_Use_Final.dim_Artist
    """
    try:
        result = get_artists_detail(artist_id)
        if not result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No artists id : {artist_id} found",
            )
        return result
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


# 將 artists_router 掛載到應用程式中，指定 /tracks 為前綴
app.include_router(artists_router, prefix="/artists")
