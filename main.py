from db import get_db
from hooks.fuzzy import fuzzy_test, fuzzy_process_test

def test_connection():
    try:
        get_db()
    except Exception as e:
        print("Connessione non riuscita!")
    print("connessione riuscita!")


