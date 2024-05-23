from typing import Optional
from fastapi import FastAPI, APIRouter, HTTPException, status, Query, Path
from app.logics.chart_logics import get_charts, get_artist_chart_detail

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
                status_code=status.HTTP_404_NOT_FOUND, detail="No Chart detail found"
            )
        return result
    except HTTPException as http_exc:
        # 404
        raise http_exc
    except Exception as e:
        # 500
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


# 某歌手或某歌曲上榜資料
@charts_router.get(
    "/{track_or_artist}/{id}",
    tags=["Charts"],
    summary="Get Specific Artists or Tracks on chart",
    operation_id="track_or_artist_on_chart_detail",
    description="Get track data of input keyword(artist or track id)",
    response_description="Specific Artists or Track data on chart",
    status_code=status.HTTP_200_OK,
)
def artist_chart_detail(
    track_or_artist: bool = Path(..., description="True for track, False for artist"),
    id: str = Path(..., description="Artist or Track ID"),
):
    """
    Get track data of input keyword(artist or track id)
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
        result = get_artist_chart_detail(track_or_artist, id)
        if not result:
            item_type = "track" if track_or_artist else "artist"
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No {item_type} found for id: {id}",
            )
        return result
    except HTTPException as http_exc:
        # 404
        raise http_exc
    except Exception as e:
        # 500
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


# 將 charts_router 掛載到應用程式中，指定 /charts 為前綴
app.include_router(charts_router, prefix="/charts")
