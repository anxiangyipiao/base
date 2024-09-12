# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class RequestItem(scrapy.Item):

        # 默认值可以在这里设置
    default_values = {
        'method': 'GET',  # 默认请求方式为 GET
        'params': None,# 默认请求参数为空
        'request_body': 'json',  # 默认请求体为空
        'cookies': None,  # 默认cookies为空
        'headers': None,  # 默认headers为空
        'meta': None,  # 默认meta为空
        'callback': None,  # 默认回调函数为空
        'errback': None,  # 默认异常回调函数为空
        'dont_filter': True,  # 默认去重
    }

    def __init__(self, *args, **kwargs):
        super(RequestItem, self).__init__(*args, **kwargs)
        for key, value in self.default_values.items():
            if key not in self:
                self[key] = value

    url = scrapy.Field()  # 请求的url
    method = scrapy.Field()  # 请求方式
    params = scrapy.Field()  # 请求参数
    request_body = scrapy.Field()  # 请求体
    cookies = scrapy.Field()  # cookies
    headers = scrapy.Field()  # headers
    meta = scrapy.Field()  # meta信息
    callback = scrapy.Field()  # 回调函数
    errback = scrapy.Field()  # 异常回调函数
    dont_filter = scrapy.Field()  # 是否去重



class BaseItem(scrapy.Item):
    """
    基础Item类，继承自scrapy.Item，用于存储爬取的基本信息。
    
    Attributes:
        source (str): 数据的来源，作为唯一标识。
        site_name (str): 网站的名称。
        province (str): 省份信息。
        city (str): 城市信息。
        county (str): 县级信息。
        title (str): 数据的标题。
        publish_time (str): 数据的发布时间。
        url (str): 详情页的链接。
        params (dict): 详情页请求所需的参数。
        method (str): 详情页请求的HTTP方法（如'GET', 'POST'等）。
    """

    source = scrapy.Field() # 来源,唯一标识
    site_name = scrapy.Field()  # 网站名称
    province = scrapy.Field()  # 省份
    city = scrapy.Field()  # 城市
    county = scrapy.Field()  # 县级
    title = scrapy.Field()  # 标题
    publish_time = scrapy.Field()  # 发布时间
    url = scrapy.Field()  # 详情页链接
    contents = scrapy.Field()  # 详情页内容
    







