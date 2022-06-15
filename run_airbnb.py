import logging
import os
import time
from datetime import datetime

import scrapy.cmdline
from google.cloud import bigquery

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)-5s - %(name)-20.20s - %(message)s - (%(filename)s:%(lineno)s)",
    handlers=[logging.StreamHandler()])
logging.getLogger("__name__").setLevel(logging.DEBUG)

# Connect to BigQuery project
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "../dev-aicam-a12eebd3222c.json"
client = bigquery.Client("dev-aicam")

# Create list of regions to search, from regions table
query = """
        SELECT region as city,SubRegion,Air_bnb as city_url FROM `dev-aicam.booking.region` 
"""
region_query = client.query(query)
regions = []
for row in region_query:
    regions.append(row[1])

def run_scrape(spider_name):
    scrape_date = datetime.now().strftime("%Y:%m:%d")
    output_str = f"weekly/{scrape_date}-nz.csv:csv"

    for region in regions:
        query_str = f"{region}, New Zealand"
        logging.debug(f"Scraping search string: '{query_str}' to {output_str}")
        scrapy.cmdline.execute(["scrapy", "crawl", f"{spider_name}", "-a", f"query={query_str}", "-o", f"{output_str}"])
        time.sleep(5)


if __name__ == "__main__":
    logging.info("Starting scrape")
    spider = "airbnb"
    run_scrape(spider)
