import logging
from datetime import date, timedelta

import scrapy

from deepbnb.api.ExploreSearch import ExploreSearch
from deepbnb.api.PdpPlatformSections import PdpPlatformSections
from deepbnb.api.PdpReviews import PdpReviews


class AirbnbSpider(scrapy.Spider):
    """Airbnb Spider

    Perform a search, collect data from search results, cache that data, then scrape each listing individually to
    obtain additional information, and finally compile the data together into a DeepbnbItem.
    """

    name = 'airbnb'
    allowed_domains = ['airbnb.com']
    default_currency = 'NZD'
    default_max_price = 1000
    default_price_increment = 40
    price_range = (0, default_max_price, default_price_increment)
    page_limit = 20

    def __init__(
            self,
            query,
            checkin=None,
            checkout=None,
            currency=default_currency,
            max_price=None,
            min_price=None,
            ne_lat=None,
            ne_lng=None,
            sw_lat=None,
            sw_lng=None,
            **kwargs
    ):
        """Class constructor."""
        super().__init__(**kwargs)
        self.__checkin = checkin
        self.__checkout = checkout
        self.__currency = currency
        self.__data_cache = {}
        self.__explore_search = None
        self.__geography = {}
        self.__ids_seen = set()
        self.__ne_lat = ne_lat
        self.__ne_lng = ne_lng
        self.__pdp_platform_sections = None
        self.__pdp_reviews = None
        self.__query = query
        self.__search_params = {}
        self.__set_price_params(max_price, min_price)
        self.__sw_lat = sw_lat
        self.__sw_lng = sw_lng

    def parse(self, response, **kwargs):
        """Default parse method."""
        data = self.__explore_search.read_data(response)
        pagination = data['data']['dora']['exploreV3']['metadata']['paginationMetadata']
        # self.logger.info(f"Pagination: {pagination['totalCount']}")
        params = {}
        self.__explore_search.add_search_params(params, response)

        if 300 < int(pagination['totalCount']):
            min_price = params['priceMin']
            max_price = params['priceMax']
            avg_price = (min_price + max_price) // 2
            self.logger.info(f"split: {pagination['totalCount']} range: {min_price} - {max_price}")

            params['priceMin'] = min_price
            params['priceMax'] = avg_price
            yield self.__explore_search.api_request(self.__query, params, self.parse)

            params['priceMin'] = avg_price
            params['priceMax'] = max_price
            yield self.__explore_search.api_request(self.__query, params, self.parse)
        else:
            # Handle pagination
            next_section = {}
            if pagination['hasNextPage']:
                items_offset = pagination['itemsOffset']
                self.__explore_search.add_search_params(next_section, response)
                next_section.update({'itemsOffset': items_offset})
                yield self.__explore_search.api_request(self.__query, next_section, response=response)
            
            params = {'key': self.__explore_search.api_key}
            self.__explore_search.add_search_params(params, response)

            self.logger.info(f'range: {params["priceMin"]} - {params["priceMax"]} paramsPagination: {pagination}')

            # handle listings
            listing_ids = self.__get_listings_from_sections(data['data']['dora']['exploreV3']['sections'])
            self.logger.info(f'Found {len(listing_ids)} listings')

            for listing_id in listing_ids:  # request each property page
                yield self.__pdp_platform_sections.api_request(listing_id)

    def price_list(self):
        prices1 = list(range(10, 1601, 20))
        prices2 = list(range(10, 1601, 30))
        prices3 = list(range(0, 1601, 40))
        price_list1 = list(zip(prices1, prices1[1:]))
        price_list2 = list(zip(prices2, prices2[1:]))
        price_list3 = list(zip(prices3, prices3[1:]))
        price_range_list = price_list1 + price_list2 + price_list3
        price_range_list.append((1500, 50000))

        return price_range_list

    def start_requests(self):
        """Spider entry point. Generate the first search request(s)."""
        # self.logger.info(f'starting survey for: {self.__query}')

        api_key = self.settings.get('AIRBNB_API_KEY')
        self.__explore_search = ExploreSearch(
            api_key,
            self.logger,
            self.__currency,
            self,
            self.settings.get('ROOM_TYPES'),
            self.__geography,
            self.__query
        )
        self.__pdp_platform_sections = PdpPlatformSections(
            self.__query,
            api_key,
            self.logger,
            self.__currency,
            self.__data_cache,
            self.__geography,
            PdpReviews(api_key, self.logger, self.__currency)
        )

        # get params from injected constructor values
        params = {}
        price_range_list = self.price_list()
        logging.debug("Searching over %s prices" % len(price_range_list))

        for min_price, max_price in price_range_list:
            self.__set_price_params(price_max=max_price, price_min=min_price)
            params['priceMin'] = min_price
            params['priceMax'] = max_price
            if self.__checkin:  # assume self._checkout also
                checkin, checkout, checkin_range_spec, checkout_range_spec = self._process_checkin_vars()
                yield from self.__explore_search.perform_checkin_start_requests(checkin, checkout, checkin_range_spec, checkout_range_spec, params)
            else:
                yield self.__explore_search.api_request(self.__query, params, self.__explore_search.parse_landing_page)

    @staticmethod
    def _get_neighborhoods(data):
        """Get all neighborhoods in an area if they exist."""
        neighborhoods = {}
        meta = data['explore_tabs'][0]['home_tab_metadata']
        if meta['listings_count'] < 300:
            return neighborhoods

        for section in meta['filters']['sections']:
            if section['filter_section_id'] != 'neighborhoods':
                continue
            for item in section['items']:
                key = item['title']
                neighborhoods[key] = item
                for param in item['params']:
                    if param['key'] == 'neighborhood_ids':
                        neighborhoods[key]['id'] = param['value']
                        break

        return neighborhoods

    def _collect_listing_data(self, listing_item: dict):
        """Collect listing data from search results, save in _data_cache. All listing data is aggregated together in the
        parse_listing_contents method."""
        listing = listing_item['listing']
        pricing = listing_item['pricingQuote']

        self.__data_cache[listing['id']] = {
            # get general data
            'avg_rating':             listing['avgRating'],
            'bathrooms':              listing['bathrooms'],
            'bedrooms':               listing['bedrooms'],
            'beds':                   listing['beds'],
            'business_travel_ready':  listing['isBusinessTravelReady'],
            'city':                   listing['city'],
            'host_id':                listing['user']['id'],
            'latitude':               listing['lat'],
            'longitude':              listing['lng'],
            'name':                   listing['name'],
            'neighborhood_overview':  listing['neighborhoodOverview'],
            'person_capacity':        listing['personCapacity'],
            'photo_count':            listing['pictureCount'],
            'photos':                 [p['picture'] for p in listing['contextualPictures']],
            'review_count':           listing['reviewsCount'],
            'room_and_property_type': listing['roomAndPropertyType'],
            'room_type':              listing['roomType'],
            'room_type_category':     listing['roomTypeCategory'],
            'star_rating':            listing['starRating'],

            'search_term':            self.__query,

            # get pricing data
            'monthly_price_factor':   pricing['monthlyPriceFactor'],
            'weekly_price_factor':    pricing['weeklyPriceFactor'],
            'price_rate':             pricing['rateWithServiceFee']['amount'],
            'price_rate_type':        pricing['rateType'],
            # use total price if dates given, price rate otherwise. can't show total price if there are no dates.
            'total_price':            pricing['price']['total']['amount'] if self.__checkin else None
        }

    def __get_listings_from_sections(self, sections: list) -> list:
        """Get listings from "sections" (i.e. search results page sections).

         Also collect some data and save it for later. Double check prices are correct, because Airbnb switches to daily
         pricing if less than 28 days are selected (e.g. during a range search).
        """
        listing_ids = []
        for section in [s for s in sections if s['sectionComponentType'] == 'listings_ListingsGrid_Explore']:
            for listing_item in section.get('items'):
                pricing = listing_item['pricingQuote']
                rate_with_service_fee = pricing['rateWithServiceFee']
                if rate_with_service_fee is None:  # some properties need dates to show rates
                    rate_with_service_fee_amt = 0
                    pricing['rateWithServiceFee'] = {'amount': None}
                else:
                    rate_with_service_fee_amt = rate_with_service_fee['amount']

                # To account for results where price_max was specified as monthly but quoted rate is nightly, calculate
                # monthly rate and drop listing if it is greater. Use 28 days = 1 month. Assume price_max of 1000+ is a
                # monthly price requirement.
                if (self.__price_max and self.__price_max > 1000
                        and pricing['rateType'] != 'monthly'
                        and (rate_with_service_fee_amt * 28) > self.__price_max):
                    continue

                self._collect_listing_data(listing_item)
                listing_ids.append(listing_item['listing']['id'])

        return listing_ids

    def _process_checkin_vars(self) -> tuple:
        """Determine if a range is specified, if so, extract ranges, validate, and return as variables.

        @NOTE: Should only be run once on crawler initialization.

        :return: checkin/checkout range specs
        """
        if not self.__checkin:
            return None, None, None, None

        checkin_range_spec, checkout_range_spec = None, None

        # Handle ranged queries
        checkin_plus_range_position = self.__checkin.find('+')
        if checkin_plus_range_position != -1:  # range_spec e.g. +5-3 means plus five days, minus three days
            checkin_range_spec = self.__checkin[checkin_plus_range_position:]
            self.__checkin = self.__checkin[:checkin_plus_range_position]

        checkout_plus_range_position = self.__checkout.find('+')
        if checkout_plus_range_position != -1:  # range_spec e.g. +-3 means plus or minus 3 days
            checkout_range_spec = self.__checkout[checkout_plus_range_position:]
            self.__checkout = self.__checkout[:checkout_plus_range_position]

        # Validate checkin / checkout values
        today = date.today()
        if date.fromisoformat(self.__checkin) < today:
            raise ValueError('Checkin cannot be in past: {}'.format(self.__checkin))
        tomorrow = today + timedelta(days=1)
        if date.fromisoformat(self.__checkout) < tomorrow:
            raise ValueError('Checkout must be tomorrow or later: {}'.format(self.__checkout))

        return self.__checkin, self.__checkout, checkin_range_spec, checkout_range_spec

    def __set_price_params(self, price_max, price_min):
        """Set price parameters based on price_max and price_min input values."""
        self.__price_max = price_max
        self.__price_min = price_min
        if self.__price_min and self.__price_max:
            self.__price_max = int(self.__price_max)
            self.__price_min = int(self.__price_min)
            self.price_range = (self.__price_min, self.__price_max, self.default_price_increment)

        if self.__price_min and not self.__price_max:
            self.__price_min = int(self.__price_min)
            self.price_range = (self.__price_min, self.default_max_price, self.default_price_increment)

        if not self.__price_min and self.__price_max:
            self.__price_max = int(self.__price_max)
            self.price_range = (0, self.__price_max, self.default_price_increment)
