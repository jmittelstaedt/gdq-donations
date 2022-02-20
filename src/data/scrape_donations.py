import sys
from time import sleep
from pathlib import Path
import logging

import pandas as pd
from requests_html import HTMLSession


GDQBASE = "https://gamesdonequick.com"
PROJECT_FOLDER = Path(__file__).resolve().parents[2]
LOG_FILE = PROJECT_FOLDER / "project.log"
REST_TIME = 0.1  # Time to rest between requests
logger = logging.getLogger(__name__)


def main():
    logger.info("Finding donation events")
    ses = HTMLSession()
    r = ses.get("/".join((GDQBASE, "tracker", "event", "cgdq")))
    num_donation_pages = {}

    for elink in filter(lambda x: "/event/" in x, r.html.links):
        event = elink.split("/")[-1]
        url = "/".join((GDQBASE, "tracker", "donations", event))
        event_data = ses.get(url)
        num_donation_pages[event] = int(
            event_data.html.find("#page", first=True).attrs["max"]
        )
        sleep(REST_TIME)

    with open(PROJECT_FOLDER / "data" / "external" / "donation_events.txt", "w") as f:
        for event in num_donation_pages.keys():
            f.write(event + "\n")

    event_counter = 0
    num_events = len(num_donation_pages.keys())
    for event, num_pages in num_donation_pages.items():
        event_counter += 1
        logger.info(
            f"Scraping donation data for {event}, {event_counter} of {num_events}"
        )
        event_fn = PROJECT_FOLDER / "data" / "external" / f"{event}_donations.csv"
        for i in range(num_pages):
            logger.info(f"Scraping {event} page {i+1} of {num_pages}")
            url = "/".join((GDQBASE, "tracker", "donations", event + f"?page={i+1}"))
            r = ses.get(url)
            df = pd.read_html(r.html.html)[0]
            df["Amount"] = df["Amount"].map(
                lambda x: float(x.strip("$").replace(",", ""))
            )
            if i == 0:
                df.to_csv(event_fn, index=False)
            else:
                df.to_csv(event_fn, mode="a", header=False, index=False)
            sleep(REST_TIME)
    logger.info("Finished scraping donations data")


if __name__ == "__main__":
    log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(
        level=logging.INFO,
        format=log_fmt,
        handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler(sys.stdout)],
    )

    main()
