import logging
from datetime import datetime

import scrapy

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)-5s - %(name)-20.20s - %(message)s - (%(filename)s:%(lineno)s)",
    handlers=[logging.StreamHandler()])
logging.getLogger("__name__").setLevel(logging.DEBUG)


def run_scrape(spider_name):
    with open("locations-nz.txt") as location_file:
        locations = location_file.read().splitlines()

    scrape_date = datetime.now().strftime("%Y:%m:%d")
    output_str = f"weekly/{scrape_date}-nz.xlsx"

    for location in locations:
        query_str = f"{location}, New Zealand"
        logging.debug(f"Scraping search string: '{query_str}' to {output_str}")
        scrapy.cmdline.execute(["scrapy", "crawl", f"{spider_name}", "-a", f"query={query_str}", "-o", f"{output_str}"])


if __name__ == "__main__":
    logging.info("Starting scrape")
    spider = "airbnb_discover"
    run_scrape(spider)
