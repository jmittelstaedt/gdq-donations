import os
import sys
import pickle
import logging
import json
from pathlib import Path

import pandas as pd

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request


PROJECT_FOLDER = Path(__file__).resolve().parents[2]
LOG_FILE = PROJECT_FOLDER / "project.log"
logger = logging.getLogger(__name__)
YT_OAUTH_FILE = PROJECT_FOLDER / "youtube_data_oauth_credentials.json"


SCOPES = ["https://www.googleapis.com/auth/youtube.force-ssl"]


def youtube_authenticate(client_secrets_file):
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
    api_service_name = "youtube"
    api_version = "v3"
    creds = None
    # the file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first time
    if os.path.exists("token.pickle"):
        with open("token.pickle", "rb") as token:
            creds = pickle.load(token)
    # if there are no (valid) credentials availablle, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                client_secrets_file, SCOPES
            )
            creds = flow.run_local_server(port=0)
        # save the credentials for the next run
        with open("token.pickle", "wb") as token:
            pickle.dump(creds, token)

    return build(api_service_name, api_version, credentials=creds)


def get_video_details(youtube, **kwargs):
    return (
        youtube.videos()
        .list(part="snippet,contentDetails,statistics", **kwargs)
        .execute()
    )


YT_INFO_COLS = [
    "id",
    "snippet.title",
    "snippet.description",
    "snippet.channelTitle",
    "snippet.tags",
    "contentDetails.duration",
    "statistics.viewCount",
    "statistics.likeCount",
    "statistics.favoriteCount",
    "statistics.commentCount",
]

YT_INFO_RENAME = {
    "snippet.title": "vod_title",
    "snippet.description": "vod_description",
    "snippet.channelTitle": "vod_channel_title",
    "snippet.tags": "vod_tags",
    "contentDetails.duration": "vod_duration",
    "statistics.viewCount": "vod_view_count",
    "statistics.likeCount": "vod_like_count",
    "statistics.dislikeCount": "vod_dislike_count",
    "statistics.favoriteCount": "vod_favorite_count",
    "statistics.commentCount": "vod_comment_count",
}


def time_to_sec(ts):
    parts = ts.split(":")
    return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])


def join_tags(tags_list):
    try:
        return ",".join(tags_list)
    except:
        return None


def main():
    logger.info("Authenticating with Google")
    youtube = youtube_authenticate(YT_OAUTH_FILE)

    logger.info("Loading run data from json")
    with open(PROJECT_FOLDER / "data" / "external" / "run_data.json", "rb") as f:
        runs = json.load(f)["data"]["runs"]

    flattened_runs = pd.json_normalize(runs)

    logger.info("Generating runs table")
    runs = flattened_runs.drop(
        [
            "runners",
            "siteCategories",
            "vods",
            "__typename",
            "event.__typename",
            "game.platform.__typename",
            "game.genre.__typename",
            "game.__typename",
        ],
        axis=1,
    )
    runs["duration"] = runs["duration"].apply(time_to_sec)
    runs.to_csv(PROJECT_FOLDER / "data" / "interim" / "GDQvods_runs.csv", index=False)

    logger.info("Generating run-runners link table")
    run_runners_temp = flattened_runs[["id", "runners"]].explode(
        "runners", ignore_index=True
    )
    run_runners = pd.json_normalize(run_runners_temp["runners"])
    run_runners["run_id"] = run_runners_temp["id"]
    run_runners = run_runners.drop("__typename", axis=1)
    run_runners.to_csv(
        PROJECT_FOLDER / "data" / "interim" / "GDQvods_run_runners.csv", index=False
    )

    logger.info("Generating run-siteCategories link table")
    run_siteCategories_temp = flattened_runs[["id", "siteCategories"]].explode(
        "siteCategories", ignore_index=True
    )
    run_siteCategories = pd.json_normalize(run_siteCategories_temp["siteCategories"])
    run_siteCategories["run_id"] = run_siteCategories_temp["id"]
    run_siteCategories = run_siteCategories.dropna()
    run_siteCategories = run_siteCategories.drop("__typename", axis=1)
    run_siteCategories.to_csv(
        PROJECT_FOLDER / "data" / "interim" / "GDQvods_run_siteCategories.csv",
        index=False,
    )

    run_vods_temp = flattened_runs[["id", "vods"]].explode("vods", ignore_index=True)
    run_vods = pd.json_normalize(run_vods_temp["vods"])
    run_vods["run_id"] = run_vods_temp["id"]
    run_vods = run_vods.drop("__typename", axis=1)
    run_vods = run_vods.explode("videoIds")
    run_vods = run_vods.rename({"videoIds": "videoId"}, axis=1)
    run_vods = run_vods.drop_duplicates()  # Some runs contain many duplicate vods

    # Get youtube information
    yt_vod_ids = list(run_vods[run_vods["source"] == "YOUTUBE"]["videoId"])
    num_yt_vods = len(yt_vod_ids)
    yt_results = []

    # We can only query up to 50 videos at a time (I think)
    for i in range(0, num_yt_vods, 50):
        logger.info(f"Getting YT vod info through {i} of {num_yt_vods}")
        yt_results.append(
            get_video_details(
                youtube, id=yt_vod_ids[i : min(i + 50, num_yt_vods)], maxResults=50
            )
        )

    logger.info("Linking youtube VOD and run information and generating run table")
    all_yt_items = sum([x["items"] for x in yt_results], start=[])
    yt_vod_info_full = pd.json_normalize(all_yt_items)
    yt_vod_info = yt_vod_info_full[YT_INFO_COLS].copy().rename(YT_INFO_RENAME, axis=1)
    yt_vod_info["vod_tags"] = yt_vod_info["vod_tags"].apply(join_tags)

    run_vods_complete = run_vods.join(yt_vod_info.set_index("id"), on="videoId")
    run_vods_complete.drop_duplicates().to_csv(
        PROJECT_FOLDER / "data" / "interim" / "GDQvods_run_vods.csv", index=False
    )  # some rows get duplicated somehow


if __name__ == "__main__":
    log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(
        level=logging.INFO,
        format=log_fmt,
        handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler(sys.stdout)],
    )

    main()
