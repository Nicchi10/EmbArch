from db import get_db
from libs.layer_one.hybrid_cache import HybridCache

def test_connection():
    try:
        get_db()
    except Exception as e:
        print("Connessione non riuscita!")
    print("connessione riuscita!")


