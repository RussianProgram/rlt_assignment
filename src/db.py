import bson
import datetime as dt
import pymongo as pm
from .conf import DB_NAME


def create_mongo_db(  # creating mongo database
    host="localhost", port=27017, db_name="mymongo"
):
    client = pm.MongoClient(host, port)
    db = client[db_name]
    return db


def get_collection():  # get mongo collection
    mongo_db = create_mongo_db(db_name=DB_NAME)
    data_collection = mongo_db[DB_NAME]

    return data_collection


def insert_bson_data(bson_path: str, collection):  # inserting data to mongo from bson
    with open(bson_path, "rb") as f:
        bson_data = f.read()

    data = bson.decode_all(bson_data)

    # collection.delete_many({})
    # print(collection.deleted_count)

    if not collection.find_one():
        collection.insert_many(data)
        print("Data inserted")
    else:
        print("Data already in db")


def get_data(collection):  # get data from mongodb
    return collection.find()


def to_isoformat(values):
    str_to_format = "-".join([f"0{v}" if v < 10 else str(v) for v in values])
    if len(values) < 3:
        str_to_format += "-01"

    date_time_raw = dt.datetime.fromisoformat(str_to_format)
    isoformatted = dt.datetime.isoformat(date_time_raw)

    return isoformatted
