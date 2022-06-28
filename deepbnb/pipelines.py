# -*- coding: utf-8 -*-
import elasticsearch.exceptions
import re
import webbrowser
import os
import pandas as pd
import numpy as np

from datetime import datetime

from deepbnb.data.big_query import UploadBigquery
from deepbnb.model import Listing
from scrapy.exceptions import DropItem
from itemadapter import ItemAdapter

class BnbPipeline:
    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            minimum_monthly_discount=crawler.settings.get('MINIMUM_MONTHLY_DISCOUNT'),
            minimum_weekly_discount=crawler.settings.get('MINIMUM_WEEKLY_DISCOUNT'),
            minimum_photos=crawler.settings.get('MINIMUM_PHOTOS'),
            skip_list=crawler.settings.get('SKIP_LIST'),
            cannot_have=crawler.settings.get('CANNOT_HAVE'),
            must_have=crawler.settings.get('MUST_HAVE'),
            property_type_blacklist=crawler.settings.get('PROPERTY_TYPE_BLACKLIST'),
            feed_format=crawler.settings.get('FEED_FORMAT'),  # output file type, autogenerated from -o file ext.
            web_browser=crawler.settings.get('WEB_BROWSER')
        )

    def __init__(
            self,
            minimum_monthly_discount,
            minimum_weekly_discount,
            minimum_photos,
            skip_list,
            cannot_have,
            must_have,
            property_type_blacklist,
            feed_format,
            web_browser
    ):
        """Class constructor."""
        self._feed_format = feed_format
        # self._fields_to_check = ['description', 'name', 'summary', 'notes']
        self._fields_to_check = ['description', 'name']
        self._minimum_monthly_discount = minimum_monthly_discount
        self._minimum_weekly_discount = minimum_weekly_discount
        self._minimum_photos = minimum_photos

        self._skip_list = skip_list
        self._property_type_blacklist = property_type_blacklist

        self._cannot_have_regex = cannot_have
        if self._cannot_have_regex:
            self._cannot_have_regex = re.compile(str(self._cannot_have_regex), re.IGNORECASE)

        self._must_have_regex = must_have
        if self._must_have_regex:
            self._must_have_regex = re.compile(str(self._must_have_regex), re.IGNORECASE)

        self._web_browser = web_browser
        if self._web_browser:
            self._web_browser = webbrowser.get(web_browser + ' %s')  # append URL placeholder (%s)

    def process_item(self, item, spider):
        """Drop items not fitting parameters. Open in browser if specified. Return accepted items."""

        if self._skip_list and str(item.get('id')) in self._skip_list:
            raise DropItem('Item in skip list: {}'.format(item['id']))

        if self._property_type_blacklist and item['room_and_property_type'] in self._property_type_blacklist:
            raise DropItem('Skipping property type: {}'.format(item['room_and_property_type']))

        if self._minimum_monthly_discount and 'monthly_discount' in item:
            if item['monthly_discount'] < self._minimum_monthly_discount:
                raise DropItem('Monthly discount too low: {}'.format(item['monthly_discount']))

        if self._minimum_weekly_discount and 'weekly_discount' in item:
            if item['weekly_discount'] < self._minimum_monthly_discount:
                raise DropItem('Weekly discount too low: {}'.format(item['weekly_discount']))

        if self._minimum_photos and item['photo_count'] < self._minimum_photos:
            raise DropItem('Photos too low: {} photos'.format(item['photo_count']))

        # check regexes
        if self._cannot_have_regex:
            for f in self._fields_to_check:
                field_val = item[f]
                if field_val is None:
                    continue
                v = str(field_val.encode('ASCII', 'replace'))
                if self._cannot_have_regex.search(v):
                    raise DropItem('Found: {}'.format(self._cannot_have_regex.pattern))

        if self._must_have_regex:
            has_must_haves = False
            for f in self._fields_to_check:
                field_val = item[f]
                if field_val is None:
                    continue
                v = str(field_val.encode('ASCII', 'replace'))
                if self._must_have_regex.search(v):
                    has_must_haves = True
                    break

            if not has_must_haves:
                raise DropItem('Not Found: {}'.format(self._must_have_regex.pattern))

        if self._web_browser:  # open in browser
            self._web_browser.open_new_tab(item['url'])

        return item

def extract_item_todict(item):
    properties = {
        'access':                 item['access'],
        'additional_house_rules': item['additional_house_rules'],
        'allows_events':          item['allows_events'],
        # 'amenities':              item['amenities'], TODO List
        'amenity_ids':            item['amenity_ids'],
        # 'avg_rating':             item['avg_rating'], TODO Problematic
        'bathrooms':              item['bathrooms'],
        'bedrooms':               item['bedrooms'],
        'beds':                   item['beds'],
        'business_travel_ready':  item['business_travel_ready'],
        'city':                   item['city'],
        'country':                item['country'],
        # 'coordinates':            {'lon': item['longitude'], 'lat': item['latitude']},
        'description':            item['description'],
        'host_id':                item['host_id'],
        # 'house_rules':            item['house_rules'], # TODO List str
        # 'interaction':            item.get('interaction'), # TODO ?
        'is_hotel':               item['is_hotel'],
        # 'monthly_price_factor':   item['monthly_price_factor'], # TODO ?
        'name':                   item['name'],
        # 'neighborhood_overview':  item['neighborhood_overview'], # TODO ?
        'person_capacity':        item['person_capacity'],
        'photo_count':            item['photo_count'],
        # 'photos':                 item['photos'], # TODO list int
        'place_id':               item['place_id'],
        # 'price_rate':             item['price_rate'], # TODO ?
        # 'price_rate_type':        item['price_rate_type'], # TODO ?
        # 'province':               item['province'], # TODO ?
        'rating_accuracy':        item['rating_accuracy'],
        'rating_checkin':         item['rating_checkin'],
        'rating_cleanliness':     item['rating_cleanliness'],
        'rating_communication':   item['rating_communication'],
        'rating_location':        item['rating_location'],
        'rating_value':           item['rating_value'],
        'review_count':           item['review_count'],
        # 'review_score':           item.get('review_score'), # TODO ?
        # 'reviews':                item.get('reviews'), # TODO List reviews
        'room_and_property_type': item['room_and_property_type'],
        'room_type':              item['room_type'],
        'room_type_category':     item['room_type_category'],
        'satisfaction_guest':     item['satisfaction_guest'],
        # 'datetime_scrape':        datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        'star_rating':            item['star_rating'],
        'state':                  item['state'],
        'transit':                item.get('transit'),
        'url':                    item['url'],
        # 'weekly_price_factor':    item['weekly_price_factor'] # TODO ?
    }
    return properties

class ElasticBnbPipeline:
    @classmethod
    def from_crawler(cls, crawler):
        return cls(elasticsearch_index=crawler.settings.get('ELASTICSEARCH_INDEX'))

    def __init__(self, elasticsearch_index):
        """Class constructor."""
        self._elasticsearch_index = elasticsearch_index

    def process_item(self, item, spider):
        """Insert / update items in ElasticSearch."""
        properties = extract_item_todict(item)

        # update if exists, else insert new
        try:
            listing = Listing.get(id=item['id'], index=self._elasticsearch_index)
            listing.update(**properties)
        except elasticsearch.exceptions.NotFoundError:
            properties['meta'] = {'id': item['id']}
            listing = Listing(**properties)
            listing.save(index=self._elasticsearch_index)

        return item


class BigqueryPipeline:
    def __init__(self, big_query: UploadBigquery):
        self.big_query = big_query

    @classmethod
    def from_crawler(cls, crawler):
        bq = UploadBigquery("dev-aicam", "booking")
        s = cls(bq)
        # crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_item(self, item, spider):
        properties = extract_item_todict(item)
        self.big_query.upload_dict(properties, "deepbnb_discover-dev")

class DuplicatesPipeline:
    """Looks for duplicate items, and drops those items that were already processed

    @ref: https://docs.scrapy.org/en/latest/topics/item-pipeline.html#duplicates-filter
    """

    def __init__(self):
        with open('locations-nz.txt') as f:
            count = sum(1 for _ in f)
        with open('locations-nz-copy.txt') as f:
            count_copy = sum(1 for _ in f)
        
        if len(os.listdir('weekly')) != 0:
            self.filepath = 'weekly/' + os.listdir('weekly')[-1]
        try:
            if count_copy < count and os.stat(self.filepath).st_size != 0:
                df = pd.read_excel(self.filepath)
                self.ids_seen = set(df['id'])
            else:
                self.ids_seen = set()
        except Exception as e:
            self.ids_seen = set()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        if np.int64(adapter['id']) in self.ids_seen:
            raise DropItem(f"Duplicate item found:")
        else:
            self.ids_seen.add(np.int64(adapter['id']))
            return item
