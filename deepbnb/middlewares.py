# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/spider-middleware.html

import scrapy
from scrapy import signals
from logging import LoggerAdapter
from scrapy import Spider
from deepbnb.api.ExploreSearch import ExploreSearch
from scrapy.selector import Selector
import logging

from scrapy.downloadermiddlewares.retry import RetryMiddleware
from scrapy.utils.response import response_status_message

import time

class TooManyRequestsRetryMiddleware(RetryMiddleware):

    def __init__(self, crawler):
        super(TooManyRequestsRetryMiddleware, self).__init__(crawler.settings)
        self.crawler = crawler

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def process_response(self, request, response, spider):
        if request.meta.get('dont_retry', False):
            return response
        elif response.status == 429:
            self.crawler.engine.pause()
            time.sleep(60) # If the rate limit is renewed in a minute, put 60 seconds, and so on.
            self.crawler.engine.unpause()
            reason = response_status_message(response.status)
            return self._retry(request, reason, spider) or response
        elif response.status in self.retry_http_codes:
            reason = response_status_message(response.status)
            return self._retry(request, reason, spider) or response
        return response 

logger = logging.getLogger(__name__)

class DeepbnbSpiderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, dict or Item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Response, dict
        # or Item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)
    
    # def process_response(self, request, response, spider):
    #     try:
    #         selector = Selector(response)
    #         # check if the page returned anything
    #         if not selector.xpath('//*[@id="menuItemButton-price_range"]/button'):
    #             scrapy.Request(url=response.url, dont_filter=True)
    #             logger.error("Error processing response %(request)s", {'request': request})
    #     except Exception as e:
    #         logger.error(e)

# retry middleware class for dont_filter=True
class RetryMiddleware(object):
    def process_response(self, request, response, spider):
        if response.status in [500, 502, 503, 504, 400, 403, 404, 408, 429]:
            logger.error("Error processing response %(request)s", {'request': request})
            return request.replace(dont_filter=True)
        else:
            return response
