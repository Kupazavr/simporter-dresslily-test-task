import sys
sys.path.append("..")
import logging
import sys
from storage.mongodb_storage import MongoDBStorage
from helpers.helpers import chunkify
from scrapers.dresslily import DresslilyParser, DresslilyScraper
from helpers.downloader_helper import Downloader
from multiprocessing.pool import ThreadPool
import gc
import pandas as pd

logging.basicConfig(format=u'%(filename)s[LINE:%(lineno)d]# %(levelname)-8s [%(asctime)s]  %(message)s',
                    level=logging.INFO)
# disable requests warnings
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("connectionpool").setLevel(logging.WARNING)


class ManagementHelper:
    def __init__(self, product_file_name, reviews_file_name):
        self.mdb = MongoDBStorage()
        self.product_file_name = product_file_name
        self.reviews_file_name = reviews_file_name
        self.domain = 'https://www.dresslily.com'
        self.downloader = Downloader(check_url=self.domain, use_proxy=True, attempts=20, use_user_agents=True)
        self.dresslily_scraper = DresslilyScraper(self.downloader, self.domain)
        self.dresslily_parser = DresslilyParser(self.downloader, self.domain)
        self.chunk_size = 300
        self.pool_size = 50

    def run(self):
        """Manage scraping, parsing and db updating"""
        logging.info('Start to scrape products')
        scraped_products = self.dresslily_scraper.scrape_products()
        logging.info('Products scraped')

        logging.info('Start upload product to db')
        self.mdb.add_products(scraped_products)

        logging.info('Getting not parsed products from db')
        not_parsed_product = list(self.mdb.product_collection.find({'reviews': {'$exists': False}},
                                                                   {'_id': 1, 'url': 1}))

        logging.info('Start to parse {} products'.format(len(not_parsed_product)))
        pool = ThreadPool(self.pool_size)
        chunks = list(chunkify(not_parsed_product, self.chunk_size))
        for n, chunk in enumerate(chunks):
            logging.info('Start to parse {}/{} chunk'.format(n+1, len(chunks)))
            parsed_chunk = list(pool.map(self.dresslily_parser.parse_single_product, chunk))

            logging.info('Chunk parsing is finished, start update product in db')
            self.mdb.add_products(parsed_chunk)
            logging.info('Start to parse reviews chunk')
            parsed_review_chunk = list(pool.map(self.dresslily_parser.parse_product_reviews, chunk))
            logging.info('Review chunk parsing is finished, start update product in db')
            self.mdb.add_products(parsed_review_chunk)
            logging.info('{}/{} chunk processing finished'.format(n + 1, len(chunks)))

        logging.info('Finish to parse dresslily')
        # Memory clearing
        pool.close()
        pool.join()
        gc.collect()
        self.make_products_csv_file()
        self.make_reviews_csv_file()

    def make_products_csv_file(self):
        """Convert db records into products csv"""
        parsed_products = self.mdb.product_collection.find({'rating': {'$exists': True}})
        to_df_products = []
        for product in parsed_products:
            processed_product = dict()
            processed_product['productId'] = product['_id']
            processed_product['productUrl'] = product['url']
            processed_product['name'] = product['name']
            processed_product['discount'] = product['discount']
            processed_product['discountedPrice'] = product['discount_price']
            processed_product['originalPrice'] = product['original_price']
            processed_product['rating'] = product['rating']
            processed_product['productInfo'] = product['product_info']
            to_df_products.append(processed_product)
        df = pd.DataFrame(to_df_products)
        df.to_csv(self.product_file_name, index=False)

    def make_reviews_csv_file(self):
        """Convert db records into reviews csv"""
        parsed_products = self.mdb.product_collection.find({'reviews': {'$gt': []}}, {'_id': 1, 'reviews': 1})
        to_df_reviews = []
        for product in parsed_products:
            for review in product['reviews']:
                processed_review = dict()
                processed_review['productId'] = product['_id']
                processed_review['rating'] = review['rating']
                processed_review['timestamp'] = review['timestamp']
                processed_review['text'] = review['text']
                processed_review['size'] = review['size']
                processed_review['color'] = review['color']
                to_df_reviews.append(processed_review)
        df = pd.DataFrame(to_df_reviews)
        df.to_csv(self.reviews_file_name, index=False)


if __name__ == '__main__':
    args = sys.argv[1:]
    product_file_name_arg = args[0]
    reviews_file_name_arg = args[1]
    mh = ManagementHelper(product_file_name_arg, reviews_file_name_arg)
    mh.run()