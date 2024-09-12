# This file is used to manage the connection to the Redis database. It is a singleton class that creates a connection pool
import json
import redis
from redis import ConnectionPool


class RedisConnectionManager:
    _pool = None

    @classmethod
    def get_pool(cls):
        if cls._pool is None:
            cls._pool = ConnectionPool(host='localhost', port=6379, db=0)
        return cls._pool

    @classmethod
    def get_connection(cls):
        return redis.Redis(connection_pool=cls.get_pool())
    

   
       



