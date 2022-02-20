import sys
import json
import logging
from subprocess import run
from pathlib import Path

import pandas as pd
from dotenv import dotenv_values, find_dotenv

PROJECT_FOLDER = Path(__file__).resolve().parents[2]
LOG_FILE = PROJECT_FOLDER / "project.log"
logger = logging.getLogger(__name__)

ENV_VARS = dotenv_values(find_dotenv())


def extract_runner_twitter():
    logger.info("Extracting runner twitter usernames")
    rr = pd.read_csv(PROJECT_FOLDER / "data" / "interim" / "GDQvods_run_runners.csv")
    twitter_unames_file = PROJECT_FOLDER / "data" / "interim" / "twitter_users.txt"
    with open(twitter_unames_file, "w") as f:
        for uname in rr[rr["twitter"].notna()]["twitter"].unique():
            if "/" in uname or uname == "bellatrix_melody":  # some entered incorrectly
                continue
            f.write(uname + "\n")

    logger.info("Scraping and saving runner twitter data")
    # This works for now, prbably better to use it as a library in the future.
    users_jsonl_fn = PROJECT_FOLDER / "data" / "interim" / "users_full.jsonl"
    run(
        [
            "twarc2.exe",
            "--bearer-token",
            ENV_VARS["bearer_token"],
            "users",
            "--usernames",
            twitter_unames_file,
            users_jsonl_fn,
        ]
    )

    # parse jsonl file to regular json
    with open(users_jsonl_fn, "r") as f:
        lines = f.readlines()
        nlines = len(lines)
        new_lines = []
        for i, line in enumerate(lines):
            if i == 0:
                new_lines.append(f"[{line},")
            elif i == nlines - 1:
                new_lines.append(f"{line}]")
            else:
                new_lines.append(f"{line},")

    with open(
        PROJECT_FOLDER / "data" / "interim" / "runner_twitter_data.json", "w"
    ) as f:
        f.writelines(new_lines)

    logger.info("Removing intermediary twitter files")
    users_jsonl_fn.unlink()  # clean up unnecessary file
    twitter_unames_file.unlink()


def main():
    extract_runner_twitter()

    with open(
        PROJECT_FOLDER / "data" / "interim" / "runner_twitter_data.json", "r"
    ) as f:
        user_data = json.load(f)

    all_data_list = []
    all_tweets_list = []
    for a in user_data:
        all_data_list += a["data"]
        all_tweets_list += a["includes"]["tweets"]

    all_data = pd.json_normalize(all_data_list).drop(
        [
            "entities.url.urls",
            "entities.description.mentions",
            "entities.description.urls",
            "entities.description.hashtags",
            "entities.description.cashtags",
        ],
        axis=1,
    )

    all_tweets = pd.json_normalize(all_tweets_list).drop(
        [
            "entities.annotations",
            "context_annotations",
            "attachments.media_keys",
            "entities.hashtags",
            "referenced_tweets",
            "entities.mentions",
            "entities.urls",
        ],
        axis=1,
    )

    all_tweets.rename(
        {x: x + "_pinned_tweet" for x in all_tweets.columns}, axis=1, inplace=True
    )

    full_data = all_data.join(
        all_tweets.set_index("id_pinned_tweet"), on="pinned_tweet_id", rsuffix="_tweet"
    )
    full_data.to_csv(
        PROJECT_FOLDER / "data" / "interim" / "GDQvods_runner_twitter_data.csv",
        index=False,
    )


if __name__ == "__main__":
    log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(
        level=logging.INFO,
        format=log_fmt,
        handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler(sys.stdout)],
    )

    main()
