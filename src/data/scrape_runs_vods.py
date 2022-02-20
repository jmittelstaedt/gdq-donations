import sys
import gzip
import json
from pathlib import Path
import logging

from seleniumwire import webdriver

VOD_URL = "https://gdqvods.com/category/all/"
PROJECT_FOLDER = Path(__file__).resolve().parents[2]
LOG_FILE = PROJECT_FOLDER / "project.log"

logger = logging.getLogger(__name__)


def main():
    logger.info("Downloading run and VOD info json using Selenium")
    ffopt = webdriver.FirefoxOptions()
    ffopt.headless = True
    driver = webdriver.Firefox(options=ffopt)
    driver.get(VOD_URL)

    logger.info("VOD webpage requested successfully")
    requests = [x for x in driver.requests if "categoryRuns" in x.url]
    if len(requests) > 1:
        logger.error("VOD requests not filtered properly! Filter update needed")
        raise ValueError("Too many VOD requests found! update filter")
    response_decoded = gzip.decompress(requests[0].response.body)
    response_json = json.loads(response_decoded)
    logger.info("Saving run and VOD info json")
    with open(PROJECT_FOLDER / "data" / "external" / "run_data.json", "w") as f:
        json.dump(response_json, f)
    driver.close()


if __name__ == "__main__":
    log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(
        level=logging.INFO,
        format=log_fmt,
        handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler(sys.stdout)],
    )

    main()
