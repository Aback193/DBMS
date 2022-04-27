import couchdb
from faker import Faker
import random

FAKE = Faker('en_US')
FAKE_DOCUMENT_SIZE = 1000000
KATEGORIJA = ['student', 'radnik', 'nezaposlen']

def db_connect() -> couchdb.Server:
    try:
        couch_server = couchdb.Server()
        couch_server.resource.credentials = ('aback', 'admin231')
        print("\nConnection to CouchDB server succesfull!")
        return couch_server
    except Exception as e:
        print("\nDB server connection error! {}".format(e))

def db_fake_populate(db_server, size) -> None:
    try:
        db = db_server.create("baza")
        for _ in range(size):
            my_dict = {'createdAt': str(FAKE.date_time_between(start_date='-50y', end_date='now')), 'ime': FAKE.name(), 'godine': random.randint(18, 90), 'adresa': FAKE.address(), 'kategorija': KATEGORIJA[random.randint(0, 2)],'komentar': FAKE.text()}
            print("\n{}".format(my_dict))
            db.save(my_dict)
    except Exception as e:
        print("\nDB already created! {}".format(e))

def db_query_simple(db_server) -> None:
    db = db_server["baza"]
    mango = db.find({"selector": {
            "godine": { "$eq": 77 }
            },
            "limit": 1000000
            })
    results = mango
    for row in results:
        print("\n {} {} {} {}".format(row['ime'], row['godine'], row['kategorija'], row['createdAt']))

def db_get_view(db_server) -> None:
    db = db_server["baza"]
    results = db.view('_design/view_all/_view/new-view')
    for row in results:
        print(row['key'])
    print("\nView size: {}".format(len(results)))



if __name__ == "__main__":
    db_server = db_connect()
    db_fake_populate(db_server, FAKE_DOCUMENT_SIZE)
    #db_query_simple(db_server)
    db_get_view(db_server)
    