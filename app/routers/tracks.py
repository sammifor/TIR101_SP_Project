from typing import Optional
from fastapi import FastAPI, APIRouter, HTTPException, status
from app.logics.track_logics import get_tracks_all, get_tracks_detail, get_tracks_number

app = FastAPI()  # 建立一個 FastAPI application

# 創建一個新的路由器（Router）用於處理 /tracks 路徑
tracks_router = APIRouter()


@tracks_router.get(
    "/count_Tracks",
    tags=["Tracks"],
    operation_id="count_Tracks",
    summary="The number of Tracks",
    description="Get the number of Tracks",
    response_description="The number of Tracks",
    status_code=status.HTTP_200_OK,
)
def count_Tracks():
    try:
        result = get_tracks_number()
        if not result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="No Tracks found"
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


@tracks_router.get(
    "/all_Tracks",
    tags=["Tracks"],
    operation_id="all_Tracks",
    summary="Get all Tracks List",
    response_description="All Tracks List",
    status_code=status.HTTP_200_OK,
)
def all_Tracks():
    """
    Get all Tracks data
    - **trackMetadata_trackUri**: track id
    - **track_name**: track name
    - **track_duration_ms**: The duration of the track in milliseconds.
    Example: 237040
    - **track_explicit**: Whether or not the track has explicit lyrics ( true = yes it does; false = no it does not OR unknown).
    - **track_popularity**: The popularity of the track. The value will be between 0 and 100, with 100 being the most popular.
    - **trackMetadata_artists_spotifyUri**: artist id

    FROM: Schema_Use_Final.dim_Tracks
    """
    try:
        result = get_tracks_all()
        if not result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="No Tracks found"
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


@tracks_router.get(
    "/{track_id}",
    tags=["Tracks"],
    operation_id="tracks_detail",
    summary="Get Specific Tracks",
    response_description="Specific Track id data",
    status_code=status.HTTP_200_OK,
)
def tracks_detail(track_id: str):
    """
    Specific Track id data

    - **trackMetadata_trackUri**: track id
    - **track_name**: track name
    - **track_duration_ms**: The duration of the track in milliseconds.
    Example: 237040
    - **track_explicit**: Whether or not the track has explicit lyrics ( true = yes it does; false = no it does not OR unknown).
    - **track_popularity**: The popularity of the track. The value will be between 0 and 100, with 100 being the most popular.
    - **trackMetadata_artists_spotifyUri**: artist id
    - **album_id**: ablum id
    - **track_end_of_fade_in**: The time, in seconds, at which the track's fade-in period ends. If the track has no fade-in, this will be 0.0.
    Example: 0
    - **track_start_of_fade_out**: The time, in seconds, at which the track's fade-out period starts. If the track has no fade-out, this should match the track's length.
    Example: 201.13705
    - **track_loudness**: The overall loudness of a track in decibels (dB). Loudness values are averaged across the entire track and are useful for comparing relative loudness of tracks. Loudness is the quality of a sound that is the primary psychological correlate of physical strength (amplitude). Values typically range between -60 and 0 db.
    Example: -5.883
    - **track_tempo**: The overall estimated tempo of a track in beats per minute (BPM). In musical terminology, tempo is the speed or pace of a given piece and derives directly from the average beat duration.
    Example: 118.211
    - **track_tempo_confidence**: The confidence, from 0.0 to 1.0, of the reliability of the tempo.
    Range: 0 - 1
    Example: 0.73
    - **acousticness**: A confidence measure from 0.0 to 1.0 of whether the track is acoustic. 1.0 represents high confidence the track is acoustic.
    Range: 0 - 1
    Example: 0.00242
    - **danceability**: Danceability describes how suitable a track is for dancing based on a combination of musical elements including tempo, rhythm stability, beat strength, and overall regularity. A value of 0.0 is least danceable and 1.0 is most danceable.
    Example: 0.585
    - **energy**: Energy is a measure from 0.0 to 1.0 and represents a perceptual measure of intensity and activity. Typically, energetic tracks feel fast, loud, and noisy. For example, death metal has high energy, while a Bach prelude scores low on the scale. Perceptual features contributing to this attribute include dynamic range, perceived loudness, timbre, onset rate, and general entropy.
    Example: 0.842
    - **instrumentalness**: Predicts whether a track contains no vocals. "Ooh" and "aah" sounds are treated as instrumental in this context. Rap or spoken word tracks are clearly "vocal". The closer the instrumentalness value is to 1.0, the greater likelihood the track contains no vocal content. Values above 0.5 are intended to represent instrumental tracks, but confidence is higher as the value approaches 1.0.
    Example: 0.00686
    - **liveness**:
    Detects the presence of an audience in the recording. Higher liveness values represent an increased probability that the track was performed live. A value above 0.8 provides strong likelihood that the track is live.
    Example: 0.0866
    - **valence**:
    A measure from 0.0 to 1.0 describing the musical positiveness conveyed by a track. Tracks with high valence sound more positive (e.g. happy, cheerful, euphoric), while tracks with low valence sound more negative (e.g. sad, depressed, angry).
    Range: 0 - 1
    Example: 0.428

    FROM: mart_metadata.dwd_tracks_with_analysis_and_features
    """
    try:
        result = get_tracks_detail(track_id.strip())
        if not result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No Tracks id : {track_id} found",
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


# 將 tracks_router 掛載到應用程式中，指定 /tracks 為前綴
app.include_router(tracks_router, prefix="/tracks")
