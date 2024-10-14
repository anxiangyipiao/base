# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals
# useful for handling different item types with a single interface
from itemadapter import is_item, ItemAdapter
from fake_useragent import UserAgent
from scrapy.downloadermiddlewares.retry import RetryMiddleware
import logging

logger = logging.getLogger(__name__)


class BaseSpiderMiddleware:
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

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


class BaseDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)



class BaseHeaderMiddleware:

    def process_request(self, request, spider):

        if request.headers.get('User-Agent') is None or "Scrapy" in request.headers.get('User-Agent').decode():
                
            request.headers['User-Agent'] = UserAgent().random

        return None


# 从start_requests中发送的请求无限重试次数
# class BaseRetryMiddleware(RetryMiddleware):
    
#         EXCEPTIONS_TO_RETRY = (TimeoutError, ConnectionRefusedError,
#                             IOError, ValueError)
    
#         def process_exception(self, request, exception, spider):        
                
#                 # if request.callback.__name__ == 'parse':
                    
#                     if isinstance(exception, self.EXCEPTIONS_TO_RETRY) \
#                             and not request.meta.get('dont_retry', False):
#                             retryreq = request.copy()
#                             retryreq.priority = request.priority + self.priority_adjust
#                             retryreq.dont_filter = True
#                             retryreq.headers['Cache-Control'] = 'no-cache'
#                             return retryreq
                    

#         def get_proxy(self, request):
#         # 根据请求动态获取代理地址
#         # 这里仅作为一个示例，实际应用中应根据具体情况来选择代理
#             proxies = [
#                 "http://proxy1.example.com:8080",
#             ]
#             return proxies[0]                
          
 
# 
class BaseRetryMiddleware(RetryMiddleware):

    EXCEPTIONS_TO_RETRY = (TimeoutError, 
                           ConnectionRefusedError,
                           IOError, ValueError)

    def process_request(self, request, spider):

        # if request.callback.__name__ == 'parse_content_detal':

        #     print('parse_content_detal')

        return None
    
    def process_response(self, request, response, spider):
         
        # if request.callback.__name__ == 'parse_content_detal':
        #      pass
        return response
   
    def process_exception(self, request, exception, spider):
        
        # if request.callback.__name__ == 'parse_content_detal':
            
            retries = request.meta.get('retry_times', 0) + 1

            if retries <= self.max_retry_times:
                logger.debug("Retrying %(request)s (failed %(retries)d times): %(reason)s",
                             {'request': request, 'retries': retries, 'reason': repr(exception)},
                             extra={'spider': spider})
                retryreq = request.copy()
                retryreq.meta['retry_times'] = retries
                retryreq.priority = request.priority + self.priority_adjust
                retryreq.dont_filter = True
                retryreq.headers['Cache-Control'] = 'no-cache'
                
                # 添加代理支持
                # proxy = self.get_proxy(request)
                # if proxy:
                #     retryreq.meta['proxy'] = proxy
                
                return retryreq
            else:
                logger.debug("Gave up retrying %(request)s (failed %(retries)d times): %(reason)s",
                             {'request': request, 'retries': retries, 'reason': repr(exception)},
                             extra={'spider': spider})

    def get_proxy(self, request):
        # 根据请求动态获取代理地址
        # 这里仅作为一个示例，实际应用中应根据具体情况来选择代理
        proxies = [
            "http://proxy1.example.com:8080",
        ]
        return proxies[0]

