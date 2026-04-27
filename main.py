from db import get_db
from libs.layer_one.hybrid_cache import HybridCache, CacheConfig
from libs.logger import setup_logging
import redis

setup_logging() 

def test_connection():
    try:
        get_db()
    except Exception as e:
        print("Ok")
    print("Not ok")

if __name__ == "__main__":
    cache = HybridCache(redis.Redis(), CacheConfig())
    print(cache.get("Come reimposto la password?"))