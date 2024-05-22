from typing import Optional
from fastapi import FastAPI, APIRouter, HTTPException, status, Query
from app.logics.chart_logics import get_charts

app = FastAPI()  # 建立一個 FastAPI application

# 創建一個新的路由器（Router）用於處理 /tracks 路徑
charts_router = APIRouter()


@charts_router.get(
    "/all_Charts",
    tags=["Charts"],
    operation_id="all_Charts",
    summary="All Charts List",
    response_description="All Charts data from 2017 - 2024.03",
    status_code=status.HTTP_200_OK,
)
def all_Charts(
    year: Optional[int] = Query(None, ge=2017, le=2024),
    month: Optional[int] = Query(None, ge=1, le=12),
    day: Optional[int] = Query(None, ge=1, le=31),
    rank: Optional[int] = Query(None, ge=1, le=200),
    limit: Optional[int] = Query(10, gt=0),
):
    """
    Get all Charts data
    - **chart_date**: 排行日期
    - **ChartEntryData_currentRank**: 該日排行
    - **trackMetadata_trackUri**: 歌曲 的 id
    - **track_name**: 歌曲名
    - **track_duration_ms**: 歌曲的長度
    - **artistUris**: 歌手 的 id
    - **artist_names**: 歌手名
    - **ChartEntryData_entryDate**: 初入榜日
    - **ChartEntryData_entryRank**: 入榜初次排名
    - **ChartEntryData_peakRank**: 上榜最高排名
    - **ChartEntryData_rankingMetric_Value**: 歌曲streaming流量
    - **labels**: 唱片公司
    - **trackMetadata_releaseDate**: 歌曲發行日

    FROM: dwd_metadata.dwd_chart_tracks_artists_genres
    """
    try:
        result = get_charts(year, month, day, rank, limit)
        if not result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="No Tracks found"
            )
        return result
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


# @charts_router.get(
#     "/{track_id}",
#     tags=["Tracks"],
#     operation_id="tracks_detail",
#     summary="Get Specific Tracks",
#     description="Get data of input Track id",
#     response_description="Specific Track id data",
#     status_code=status.HTTP_200_OK,
# )
# def tracks_detail(track_id: str):
#     try:
#         result = get_tracks_detail(track_id)
#         if not result:
#             raise HTTPException(
#                 status_code=status.HTTP_404_NOT_FOUND,
#                 detail=f"No Tracks id : {track_id} found",
#             )
#         return result
#     except Exception as e:
#         raise HTTPException(
#             status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
#         )


# 將 charts_router 掛載到應用程式中，指定 /charts 為前綴
app.include_router(charts_router, prefix="/charts")
