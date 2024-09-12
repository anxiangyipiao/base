# Desc: 布隆过滤器
import mmh3
import math
from base.settings import BLOOMFILTER_KEY
from base.utils.RedisManage import RedisConnectionManager
from datetime import datetime


from functools import lru_cache

# 常量定义
HASH_SEED_INCREMENT = 2

class SimpleHash(object):
    def __init__(self, capacity, seed):
        self.capacity = capacity
        self.seed = seed

    def hash(self, value:str):
        """Compute the hash value."""
        if not isinstance(value, str):
            value = str(value)
        ret = 0
        for char in value:
            ret += self.seed * ret + ord(char)
        return ret % self.capacity

class BloomFilter(object):
    def __init__(self, server, expected_items, false_positive_rate, key):
        """
        初始化布隆过滤器实例。
        
        Args:
            server (str): 远程服务器的地址。
            expected_items (int): 预计插入布隆过滤器的元素数量。
            false_positive_rate (float): 误报率，取值范围在0到1之间。
            key (str, optional): 布隆过滤器在Redis中的key。默认为"bloomfilter"。
        
        Returns:
            None
        
        Raises:
            ValueError: 如果误报率不在0到1之间或预计插入的元素数量小于0，则抛出此异常。
        """
        if false_positive_rate >= 1 or false_positive_rate <= 0:
            raise ValueError("False positive rate must be between 0 and 1")
        if expected_items < 0:
            raise ValueError("Expected items count must be non-negative")

        # 计算位数组大小
        self.bit_size = int(-expected_items * math.log(false_positive_rate) / (math.log(2) ** 2))
        # 计算哈希函数数量
        self.hash_count = max(1, int((self.bit_size / expected_items) * math.log(2)))
        self.key = self.month_reset(key)
        self.server = server
        # 初始化哈希函数
        self.hash_functions = [
            SimpleHash(self.bit_size, seed + HASH_SEED_INCREMENT * index)
            for index, seed in enumerate(range(self.hash_count))
        ]

    def is_contained(self, str_input):
        """
        判断字符串是否可能存在于过滤器中。
        
        Args:
            str_input (str): 待判断的字符串。
        
        Returns:
            bool: 如果字符串可能存在于过滤器中，则返回True；否则返回False。
        
        """
        """Check if the item might be in the filter."""
        if not str_input:
            return False
        hash_value = self._get_hash(str_input)
        for func in self.hash_functions:
            location = func.hash(hash_value)
            if not self.server.getbit(self.key, location):
                return False
        return True

    def add(self, str_input):
        """
        向过滤器中添加一个元素。
        
        Args:
            str_input (str): 待添加的字符串元素。
        
        Returns:
            None
        
        """
        """Add an item to the filter."""
        if not str_input:
            return
        hash_value = self._get_hash(str_input)
        for func in self.hash_functions:
            location = func.hash(hash_value)
            self.server.setbit(self.key, location, 1)

    def month_reset(self,key):
        """
        根据当前月份重置布隆过滤器key。
        
        Args:
            key (str): 需要重置的布隆过滤器key。
        
        Returns:
            str: 拼接当前月份后的布隆过滤器key。
        
        """
        month = datetime.now().month # 当前月份
        BLOOMFILTER_KEY = key +":" + str(month)  # 生成布隆过滤器key
        return BLOOMFILTER_KEY

    @lru_cache(maxsize=1024)
    def _get_hash(self, url: str) -> str:
        """
        计算 URL 的 MurmurHash 哈希值，并使用 LRU 缓存机制。
        """
        return str(mmh3.hash(url) % self.bit_size)


# 示例使用
bloomFilter = BloomFilter(server=RedisConnectionManager.get_connection(),expected_items=10000000, false_positive_rate=0.001,key=BLOOMFILTER_KEY)
