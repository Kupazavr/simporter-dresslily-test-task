from pymongo import MongoClient, UpdateOne
from helpers.helpers import parse_config
import ast


class MongoDBStorage:
    def __init__(self):
        self.config = parse_config('db')
        self.client = self.connect_to_db()
        self.product_collection = self.client[self.config['PRODUCTS_COLLECTION']]

    def connect_to_db(self):
        # if test_env variable set as True - connect to localhost
        if ast.literal_eval(self.config['TEST_ENV']) is True:
            client = MongoClient('mongodb://localhost', connect=False)[self.config['NAME']]
        else:
            client = MongoClient('mongodb://{login}:{password}@{ip}/{db_name}'.format(login=self.config['LOGIN'],
                                                                                      password=self.config['PASSWORD'],
                                                                                      ip=self.config['IP'],
                                                                                      db_name=self.config['NAME']),
                                 connect=False)[self.config['NAME']]
        return client

    def add_products(self, projects):
        # creating UpdateOne instants for faster batch update
        docs = [UpdateOne({'_id': post['_id']}, {'$set': post}, upsert=True) for post in projects]
        if docs:
            self.client[self.config['PRODUCTS_COLLECTION']].bulk_write(docs)




