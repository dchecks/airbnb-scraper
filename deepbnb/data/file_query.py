
class FileQuery:
    def __init__(self, absolute_folder):
        self.absolute_folder = absolute_folder

    def readDiscover(self, website, out_table):
        return []

    def readRegion(self, website, status):
        return [{
            "city": "Northland",
            "SubRegion": "Kaipara District",
            "city_url": "https://www.airbnb.co.nz/s/Kaipara-District--Northland--New-Zealand/homes?"
        }
        # {
        #     "city": "Wanaka",
        #     "city_url": "https://www.airbnb.co.nz/s/Wanaka--Otago/homes?tab_id=home_tab&refinement_paths%5B%5D=%2Fhomes&source=structured_search_input_header&search_type=autocomplete_click&query=Wanaka%2C%20Otago&place_id=ChIJby3suR1G1akR4MF5hIbvAAU",
        # }
        ]

    def upload(self, data, out_table):
        filename = f"/{self.absolute_folder}/{out_table}.json"
        with open(filename, "a") as f:
            for line in data:
                f.write(str(line) + '\n')
