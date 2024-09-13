# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import json
from itemadapter import ItemAdapter
from .items import BaseItem
from base.utils.RedisManage import RedisConnectionManager


class BasePipeline:

    def __init__(self) -> None:
        
        self.redis = RedisConnectionManager().get_connection()


    def process_item(self, item, spider):

        # 判断是否是BaseItem的实例
        if isinstance(item, BaseItem):
            
            # 将item转换为字典
            item_dict = dict(item)
            # 将item_dict转换为json字符串
            item_json = json.dumps(item_dict, ensure_ascii=False)
            # 将item_json存入redis
            self.redis.lpush('result', item_json)
        

        return item
