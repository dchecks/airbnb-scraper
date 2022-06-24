import datetime
import logging


class ParseJson:
    def parse_json(self, response, id, region, url, sub_region):
        logging.debug(f"Parsing json for id: {id}")
        scraping_time = datetime.now()  # .strftime("%Y-%m-%d %X")
        listing = {}
        pictures = []

        try:
            self.exist_id_lists.append(id)
            data = response.json()

            # Check in NZ
            if "pdp_listing_detail" not in data:
                logging.debug(f"No pdp_listing_detail element found for the id: {id}")
                return

            data = data["pdp_listing_detail"]

            if self.in_location(data):
                listing["website"] = self.website
                listing["region"] = region
                listing["subregion"] = sub_region
                listing["scraping_time"] = scraping_time
                listing["starting_url"] = url
                listing["id"] = id
                listing["name"] = data["name"]
                listing["location"] = data["location_title"]
                listing["city"] = data["localized_city"]
                listing["type"] = data["room_and_property_type"]
                listing["lat"] = data["lat"]
                listing["lng"] = data["lng"]
                listing["rating"] = data["reviews_module"]["localized_overall_rating"]
                listing["review_count"] = data["review_details_interface"]["review_count"]
                listing["host_name"] = data["primary_host"]["host_name"]
                listing["host_id"] = data["primary_host"]["id"]
                description = data.get("sectioned_description")
                if description:
                    for key in [
                        "description",
                        "access",
                        "space",
                        "notes",
                        "transit",
                        "summary",
                        "house_rules",
                        "interaction",
                        "neighborhood_overview",
                    ]:
                        if key in description:
                            listing[key] = description[key]
                if "guest_label" in data:
                    listing["guests"] = data["guest_label"].split(" ")[0]
                if "bathroom_label" in data:
                    listing["bathrooms"] = data["bathroom_label"].split(" ")[0]
                if "bedroom_label" in data:
                    listing["bedrooms"] = data["bedroom_label"].split(" ")[0]
                if "bed_label" in data:
                    listing["beds"] = data["bed_label"].split(" ")[0]
                if data.get("photos"):
                    for photo in data["photos"]:
                        listing_picture = {}
                        listing_picture["id"] = listing["id"]
                        listing_picture["picture"] = photo["large"].split("?")[0]
                        pictures.append(listing_picture)
                logging.info(listing["name"])
            else:
                logging.error(f'wrong location {data["location_title"]}')

        except: logging.debug("parse json failed")
