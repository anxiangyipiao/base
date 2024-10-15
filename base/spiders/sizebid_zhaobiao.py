import time
from .base import BaseListSpider,RequestItem



class Henan_Pindingshan_ggzy_zhaobiaoSpider(BaseListSpider):
    # ggzy: 公共资源网     zfcg：政府采购
    name = "sizebid_zhaobiao"
    start_urls = 'http://m.sizebid.com/bid-information/{page}.html?fuzzySearch=false'
    
    next_base_urls = ''  # 用于下一页网址拼接
    contents_base_urls = ''  # 用于拼接详情页网址

    province = ""  # 必填，爬虫省份
    city = ""  # 必填，爬虫城市
    county = ""  # 选填，爬虫区/县
    site_name = '势必得招标网'
    source = 'm.sizebid.com'

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
    }

   
    def start_requests(self):


             # 设置请求参数
            request_params = {
                'request_body': None,
                'url': self.start_urls.format(page=62),
                'method': 'GET',
                'meta': {'page': 62},
                'callback': self.parse,
                'cookies': None,
                'headers': None,
                'params': None
            }

            yield self.parse_task(RequestItem(**request_params))
        
    def parse(self, response):
        
        page = response.meta['page']

        node_list  = response.xpath('//div[@class="row"]')
 
        for node in node_list:

            baseItem = self.get_base_item()
            baseItem['title'] = node.xpath('./a/span/text()').extract_first().strip()
            baseItem['publish_time'] = node.xpath('./span/text()').extract_first().strip()
            baseItem['url'] = self.contents_base_urls + node.xpath('./a/@href').extract_first().strip()
          
            request_params = {
                'url': baseItem['url'],
                'meta': {'item': baseItem},
                'callback': self.parse_content_detal,
                'errback': self.errback_httpbin, 
            }

            # 判断是否继续爬取
            if self.calculate_task_item(baseItem):
                # 爬取详情页
                yield self.parse_task(RequestItem(**request_params))
      
        
        page = page + 1
        urls = self. start_urls.format(page = page)
        request_params = {
                'request_body': None,
                'url': urls,
                'method': 'GET',
                'meta': {'page': page},
                'callback': self.parse,
                'params': None
            }
        
        # 爬取下一页
        yield self.parse_task(RequestItem(**request_params))

