from db import get_db
from libs.layer_one.hybrid_cache import HybridCache, CacheConfig
from libs.logger import setup_logging
import redis
import json

setup_logging() 

def test_connection():
    try:
        get_db()
    except Exception as e:
        print("Ok")
    print("Not ok")

if __name__ == "__main__":
    cache = HybridCache(redis.Redis(), CacheConfig())

    # with open("test.json", encoding="utf-8") as f:
    #     data = json.load(f)

    # for faq in data["faqs"]:
    #     try:
    #         norm = cache.set(
    #             query=faq["query"],
    #             answer=faq["answer"],
    #             faq_id=faq["faq_id"],
    #         )
    #         print(f"[OK] {faq['faq_id']:>3} -> {norm!r}")
    #     except ValueError as e:
    #         print(f"[SKIP] {faq['faq_id']:>3} -> {e} (query: {faq['query']!r})")

    print(cache.get("Come reimposto la password?"))