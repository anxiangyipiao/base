import json
from scrapy.exceptions import CloseSpider
import scrapy
from base.utils.BloomFilter import bloomFilter
from base.utils.RedisManage import RedisConnectionManager
from datetime import datetime
from base.items import BaseItem,RequestItem
from scrapy import signals
import logging
import re


logger = logging.getLogger(__name__)

class BaseListSpider(scrapy.Spider):

    name = "base"
    start_urls = []

    next_base_urls = ''  # 用于下一页网址拼接
    contents_base_urls = None  # 用于拼接详情页网址
    province = None  # 必填，爬虫省份
    city = None  # 必填，爬虫城市
    county = None  # 选填，爬虫区/县
    site_name = None
    source = None # 网站
    
    timeRange = 10
    crawl_today = datetime.now() # 爬虫开始时间
    end_time = None # 爬虫结束时间
    insertCount = 0 # 总任务数量
    successCount = 0 # 成功数量
  
    task_redis_server = RedisConnectionManager.get_connection(db=0) # Redis连接
   

    '''# @classmethod
    # def from_crawler(cls, crawler, *args, **kwargs):
    #     """
    #     从 Crawler 创建 spider 实例，并连接信号处理器。
        
    #     Args:
    #         cls (type): Spider 的类对象。
    #         crawler (Crawler): Scrapy 引擎的 Crawler 实例。
        
    #     Returns:
    #         BaseSpider: 创建的 Spider 实例。
    #     """
    #     spider = super(BaseSpider, cls).from_crawler(crawler, *args, **kwargs)
    #     crawler.signals.connect(spider.closed, signal=signals.spider_closed)
    #     return spider
    '''
    
    def get_base_item(self)->BaseItem:
        """
        返回一个包含基本信息的 BaseItem 对象。
        
        Args:
            无。
        
        Returns:
            BaseItem: 包含以下基本信息的 BaseItem 对象：
                - source: 数据来源
                - site_name: 站点名称
                - province: 省份
                - city: 城市
                - county: 区县
        
        """
        """
        返回 BaseItem 对象。
        
        Args:
            baseItem (BaseItem): 待返回的 BaseItem 对象。
        
        Returns:
            BaseItem: 返回的 BaseItem 对象。
        
        """

        baseItem = BaseItem()
        baseItem['source'] = self.source
        baseItem['site_name'] = self.site_name
        baseItem['province'] = self.province
        baseItem['city'] = self.city
        baseItem['county'] = self.county

        return baseItem

    # 判断时间超过timeRange天的url不再爬取
    def is_time_out(self, time:datetime)->bool:
        """
        判断给定的时间是否超出了设定的时间范围。
        
        Args:
            time (datetime.datetime): 待判断的时间点。
        
        Returns:
            bool: 若给定的时间点超出了设定的时间范围，则返回True；否则返回False。
        
        """
        if abs((time - self.crawl_today).days) > self.timeRange:
            return True

        return False
    
    # 提取数字
    def extract_number(self, string:str)->str:
        """
        从字符串中提取2024-11-12。
        
        Args:
            string (str): 待提取的字符串。
        
        Returns:
            str: 提取出的数字字符串。
        
        """
        try:
            number = re.search(r'\d{4}-\d{2}-\d{2}', string).group()
        except:
            number = None
        return number

    def format_time(self, publish_time)->datetime:
        """
        格式化时间字符串，将发布时间转换为 datetime 对象。
        
        Args:
            publish_time (str): 发布时间字符串，格式为年月日时分秒或年月日等。
        
        Returns:
            datetime: 格式化后的 datetime 对象，格式为 '%Y-%m-%d'。
        
        """
        

        if '/' in publish_time:
            publish_time = publish_time.replace('/', '-')
        if ' ' in publish_time:
            publish_time = publish_time.replace(' ', '')
        if '.' in publish_time:
            publish_time = publish_time.replace('.', '-')
        if '[' in publish_time:
            publish_time = publish_time.replace('[', '')
        if ']' in publish_time:
            publish_time = publish_time.replace(']', '')
        if '年' in publish_time:
            publish_time = publish_time.replace('年', '-')    
        if '月' in publish_time:
            publish_time = publish_time.replace('月', '-')
        if '日' in publish_time:
            publish_time = publish_time.replace('日', '')


        publish_time = self.extract_number(publish_time)

        if len(publish_time) > 10:
            publish_time = publish_time[:10]

        try:
            time = datetime.strptime(str(publish_time), '%Y-%m-%d')

        except:

            logger.error("Time format error")

        return time
    
    def is_url_having(self, url:str)->bool:
        """
        判断给定的URL是否在布隆过滤器中。
        
        Args:
            url (str): 待判断的URL。
        
        Returns:
            bool: 若URL在布隆过滤器中，返回True；否则返回False
        """
        
        if bloomFilter.is_contained(url):
            return True
        
        return False
    
    def add_url(self,url:str):
        """
        将给定的URL添加到布隆过滤器中。
        
        Args:
            url (str): 待添加的URL。
        
        Returns:
            None
        """
        
        try:
            bloomFilter.add(url)
        except:
            logger.error("BloomFilter add error")

    def is_time_stop(self,publishTime:str)->bool:
        """
        判断当前时间是否超过了发布时间所指定的时间限制
        
        Args:
            publishTime (str): 发布时间，格式为"%Y-%m-%d %H:%M:%S"
        
        Returns:
            bool: 如果当前时间超过了发布时间所指定的时间限制，返回True；否则返回False
        """
  
        time = self.format_time(publishTime)
        
        return self.is_time_out(time)
    
    def calculate_task_item(self,task:BaseItem):
        """

        Args:
            task (BaseItem): 待插入的任务对象，需为BaseItem或其子类的实例。

        Returns:
            None

        """

        # 检查task['url'],task['publish_time'] 是否为空
        if task['url'] is None:
            # 将url插入到url_error队列
            self.insert_url_error()
            raise CloseSpider('url xpath is changed')
        if task['publish_time'] is None:
            # 将time插入到time_error队列
            self.insert_time_error()
            raise CloseSpider('time xpath is changed')

        # 检查任务是否满足停止条件,如果时间超过timeRange天则停止爬取
        if self.is_time_stop(task['publish_time']):
            
            self.log("Stopping spider due to time condition.")
            raise CloseSpider(reason='Time Stop Condition Met')
 
        # 检查任务是否满足停止条件,如果url已经爬取过则跳过
        if self.is_url_having(task['url']):
            self.log("Url exit.")
            return False

        # 计算任务数量
        try:           
                self.insertCount += 1
        except Exception as e:
                self.insertCount -= 1
                logger.error("Insert task item error",e)

        return True

    def insert_url_error(self):
        
        try:
            data = {
                'source': self.source,
                'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            }
            self.task_redis_server.lpush('url_error',json.dumps(data))
        
        except:
            logger.error("Insert url queue error")

    def insert_time_error(self):
        
        try:
            data = {
                'source': self.source,
                'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            }
            self.task_redis_server.lpush('time_error',json.dumps(data))
        except:
            logger.error("Insert time queue error")

    def get_key(self):
        return  "task_log:" + self.source + ":" + self.crawl_today.strftime('%Y-%m-%d')

    def init_source_log(self,key):
       
        # 参数1 今天网站更新的总数量
        # 参数2 今天网站爬取的成功数量
        # 参数3 今天网站爬取的失败数量 
        # 参数4 今天的日期
        # 参数5 最近一次爬取的时间
        # 参数6 今天爬取的次数
        # key 为 source + 日期

        data = {
            'time': self.crawl_today.strftime('%Y-%m-%d'),
            'all_request': 0,
            'success_request': 0,
            'fail_request': 0,
            'last_time': '',
            'crawl_count': 0,
        }

        # hash 结构存储

        if not self.task_redis_server.exists(key):
                
            self.task_redis_server.hmset(key, data)
       
    def read_source_log(self,key):

        data = self.task_redis_server.hgetall(key)

        # 转为字典
        return {
            'time': data[b'time'].decode('utf-8'),
            'all_request': int(data[b'all_request'].decode('utf-8')),
            'success_request': int(data[b'success_request'].decode('utf-8')),
            'fail_request': int(data[b'fail_request'].decode('utf-8')),
            'last_time': data[b'last_time'].decode('utf-8'),
            'crawl_count': int(data[b'crawl_count'].decode('utf-8'))
        }
  
    def write_source_log(self,key,data:dict):

        self.task_redis_server.hmset(key, data)

    def insert_task_log(self):
        """
        插入任务日志
        
        Args:
            无参数
        
        Returns:
            无返回值
        
        Raises:
            无异常抛出
        
        """
        key = self.get_key()

        # 初始化日志
        self.init_source_log(key)

        # 读取日志
        data = self.read_source_log(key)

        # 计算总数量
        data['all_request'] =data['all_request'] +  self.insertCount -  data['fail_request']

        # 计算成功数量
        data['success_request'] += self.successCount

        # 计算失败数量
        data['fail_request'] = data['all_request'] - data['success_request']

        # 计算最近一次爬取时间
        data['last_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # 计算爬取次数
        data['crawl_count'] += 1


        # 写入日志
        self.write_source_log(key,data)


        # 输出日志
        logger.info("source: %s, time: %s, all_request: %d, success_request: %d, fail_request: %d, last_time: %s, crawl_count: %d" % (self.source,data['time'],data['all_request'],data['success_request'],data['fail_request'],data['last_time'],data['crawl_count']))
        
    # 爬虫关闭时调用
    def closed(self, reason):

        # if reason == 'Time Stop Condition Met':
            
            self.insert_task_log()

    def parse_task(self,tasks:RequestItem):

        if tasks['method'].upper() == 'GET':

            if tasks['params'] is None:
                return scrapy.Request(tasks['url'],method='get',callback=tasks['callback'],dont_filter=True,meta=tasks['meta'],cookies=tasks['cookies'],headers=tasks['headers'])
            else:
                return scrapy.Request(tasks['url'],method='get',callback=tasks['callback'],dont_filter=True,meta=tasks['meta'],cookies=tasks['cookies'],headers=tasks['headers'],body=json.dumps(tasks['params']))

        if tasks['method'].upper() == 'POST':

            if tasks['request_body'].lower() == 'formdata':
                return scrapy.FormRequest(tasks['url'],method='post',callback=tasks['callback'],dont_filter=True,meta=tasks['meta'],cookies=tasks['cookies'],headers=tasks['headers'],formdata=tasks['params'])

            elif tasks['request_body'].lower() == 'json':
                return scrapy.Request(tasks['url'],method='post',callback=tasks['callback'],dont_filter=True,meta=tasks['meta'],cookies=tasks['cookies'],headers=tasks['headers'],body=json.dumps(tasks['params']))

    def parse(self,response):
        pass

    def parse_content_detal(self,response):

         # 获取详情页数据
        item:BaseItem = self.parse_content(response)  
        
        # 计算成功数量
        self.successCount += 1

        # 将url插入到布隆过滤器
        self.add_url(item['url'])

        yield item
    
    def parse_content(self,response)->BaseItem:
         

        # response 返回的类型包括html和json两种格式
        # 处理html格式
        item = response.meta['item']
        
        try:

            if response.xpath('//iframe'):
                # 获取pdf文件地址
                req_url = response.xpath('//iframe/@src').extract_first()
                item['contents'] = req_url
            
            else:
                # 获取html页面内容
                item['contents'] = response.text

            return item

        except Exception as e:

            logger.error("Parse content error",e)
            return None