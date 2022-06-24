import json

from deepbnb.data.parse_json import ParseJson


class TestJsonParse:

    def test_text_to_json(self):
        with open("resources/sample_response.txt", "r") as f:
            data = f.readlines()
        data = str(data)
        print(data)
        # print(json.loads(data))
