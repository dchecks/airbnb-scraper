import logging
import time

import scrapy.cmdline
from google.cloud import bigquery

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)-5s - %(name)-20.20s - %(message)s - (%(filename)s:%(lineno)s)",
    handlers=[logging.StreamHandler()])
logging.getLogger("__name__").setLevel(logging.DEBUG)

def retrieve_locations():
    # Connect to BigQuery project
    client = bigquery.Client("dev-aicam")

    # Create list of regions to search, from regions table
    query = """
            SELECT SubRegion FROM `dev-aicam.booking.region` 
    """

    region_query = client.query(query)
    regions = []
    for row in region_query:
        regions.append(row[0])

    return regions

def run_scrape(spider_name):
    regions = retrieve_locations()

    # TODO Debug
    regions = regions[:1]

    for region in regions:
        query_str = f"{region}, New Zealand"
        logging.debug(f"Scraping search string: '{query_str}'")
        scrapy.cmdline.execute(["scrapy", "crawl", f"{spider_name}", "-a", f"query={query_str}"])
        time.sleep(5)


if __name__ == "__main__":
    logging.info("Starting scrape")
    spider = "airbnb"
    run_scrape(spider)
