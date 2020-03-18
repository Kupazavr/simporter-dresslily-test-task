import sys
sys.path.append("..")
from bs4 import BeautifulSoup
import logging
import traceback
from multiprocessing.pool import ThreadPool
from functools import reduce
import re
import datetime
import gc


class DresslilyScraper:
    def __init__(self, downloader, domain):
        self.downloader = downloader
        self.domain = domain
        self.hoodie_page_url = self.domain + '/hoodies-c-181-page-{}.html'

    @staticmethod
    def get_products_on_category_page(page_soup):
        """
        Parse products on category page
        :param page_soup: page soup with products
        :type page_soup: BeautifulSoup
        :return: list of products on page
        :rtype: list
        """
        products = page_soup.find_all('div',
                                      class_='js-good js-dlGood js_logsss_browser js_logsss_event_ps category-good')
        return products

    @staticmethod
    def get_pages_count(page_soup, can_be_error=False):
        """
        Get pages count in pagination block
        :param page_soup: category page soup
        :type page_soup: BeautifulSoup
        :param can_be_error: if review page, can be no pages_count bacause reviews < 6
        :type can_be_error: bool
        :return: pages count
        :rtype: int, None
        """
        pages_count = None
        try:
            pagination_block = page_soup.find('div', class_='site-pager')
            last_page_tag = pagination_block.find_all('li')[-2]
            try:
                pages_count = int(last_page_tag.text)
            except AttributeError:
                logging.error('No pages count')
        except Exception as e:
            if not can_be_error:
                logging.error('Except on getting pages count')
        return pages_count

    @staticmethod
    def get_product_id(product):
        """
        Getting id from product link
        :param product: single product soup object
        :type product: BeautifulSoup
        :return: product id
        :rtype: int
        """
        _id = int(product.a['href'].split('product')[-1].replace('.html', ''))
        return _id

    @staticmethod
    def get_product_url(product):
        """
        :type product: BeautifulSoup
        :rtype: str
        """
        url = product.a['href']
        return url

    @staticmethod
    def get_product_name(product):
        """
        :type product: BeautifulSoup
        :rtype: str
        """
        name = product.find('a', class_='goods-name-link js_logsss_click_delegate_ps').text
        return name

    @staticmethod
    def get_prices(product):
        """
        Getting price, discount price and calculate sale percentage
        :param product: single product soup object
        :type product: BeautifulSoup
        :return: original price, discount price and discount percentage
        :rtype: tuple
        """
        original_price = float(
            product.find('span', class_='my-shop-price category-good-price-market dl-has-rrp-tag')['data-orgp'])
        discount_price = float(
            product.find('span', class_='js-dlShopPrice my-shop-price category-good-price-sale')['data-orgp'])
        # calculate discount
        discount = round((100 * (original_price - discount_price)) / original_price)
        return original_price, discount_price, discount

    def scrape_single_product(self, product):
        """
        Scrape all data from product on category page
        :param product: single product soup object
        :type product: BeautifulSoup
        :return: product primary attributes
        :rtype: dict
        """
        product_info = {}
        try:
            product_info['_id'] = self.get_product_id(product)
            product_info['url'] = self.get_product_url(product)
            product_info['name'] = self.get_product_name(product)
            product_info['original_price'], \
            product_info['discount_price'], \
            product_info['discount'] = self.get_prices(product)
            return product_info
        except Exception as e:
            logging.error(traceback.format_exc())
            logging.error('Receive exception on product scraping')
            return None

    def get_link_products(self, link):
        """
        Scrape all products from category link
        :param link: category page link
        :type link: str
        :return: scraped page products
        :rtype: list
        """
        response = self.downloader.get(link)
        soup = BeautifulSoup(response, 'lxml')
        scraped_products = self.scrape_category_page(soup)
        return scraped_products

    def scrape_category_page(self, soup):
        """
        Scrape category page soup
        :param soup: category page soup
        :type soup: BeautifulSoup
        :return: scraped page products
        :rtype: list
        """
        products = self.get_products_on_category_page(soup)
        scraped_products = [product for product in map(self.scrape_single_product, products) if product]
        return scraped_products

    def scrape_products(self):
        """
        Scraping all pages from category
        :return: scraped page products
        :rtype: list
        """
        all_products = []
        # parse first page separately because need to get pages count
        first_page_response = self.downloader.get(self.hoodie_page_url.format(1))
        if not first_page_response:
            logging.error('Can`t get first page')
            return all_products
        first_page_soup = BeautifulSoup(first_page_response, 'lxml')
        # get products from first page
        first_page_products = self.scrape_category_page(first_page_soup)
        all_products.extend(first_page_products)
        logging.info('first page scraped')
        pages_count = DresslilyScraper.get_pages_count(first_page_soup)
        if not pages_count:
            logging.error('Found no pages on category scraping')
            return all_products
        logging.info('found {} pages'.format(pages_count))
        # parse all pages
        pages_links = map(lambda page: self.hoodie_page_url.format(page), range(2, pages_count + 1))
        pool = ThreadPool(50)
        pool_products_result = list(pool.map(self.get_link_products, pages_links))
        # concat pool result in flat list
        scraped_products = reduce(lambda first, second: first + second, pool_products_result)
        # clear memory
        pool.close()
        pool.join()
        gc.collect()
        all_products.extend(scraped_products)
        return all_products


class DresslilyParser:
    def __init__(self, downloader, domain):
        self.downloader = downloader
        self.domain = domain
        self.review_pattern = self.domain + '/m-review-a-view_review-goods_id-{}-page-{}.htm'

    def parse_single_product(self, product):
        """
        Updating product with inner page data
        :param product: product data from db
        :type product: dict
        :return: updated product
        :rtype: dict
        """
        response = self.downloader.get(product['url'])
        soup = BeautifulSoup(response, 'lxml')
        product['rating'] = self.get_product_rating(soup)
        product['product_info'] = self.get_product_info(soup)
        logging.debug('{} product parsed'.format(product['_id']))
        return product

    @staticmethod
    def get_product_info(product_soup):
        """
        Get product info in string format
        :param product_soup: product page soup
        :type product_soup: BeautifulSoup
        :return: product info in string format
        :rtype: str
        """
        product_info = {}
        product_info_block_keys = product_soup.find('div', class_='xxkkk20').find_all('strong')
        # parse table in product info dict and than convert it in string
        for product_info_key in product_info_block_keys:
            product_info[product_info_key.text.replace(':', '').strip()] = product_info_key.next.next.strip()
        product_info_string = ';'.join([f'{k}:{v}' for k, v in product_info.items()])
        return product_info_string

    @staticmethod
    def get_product_rating(product):
        """
        :type product: BeautifulSoup
        :rtype: float, None
        """
        try:
            rating = float(product.find('span', class_='review-avg-rate').text)
        except AttributeError:
            # if product has no rating
            rating = None
        return rating

    def parse_product_reviews(self, product):
        """
        Parse product reviews
        :type product: product from db
        :rtype: dict
        """
        product['reviews'] = self.get_product_reviews(product['_id'])
        return product

    def get_product_reviews(self, product_id):
        """
        Getting all product reviews
        :param product_id: product id
        :type product_id: int, str
        :return: list with all reviews
        :rtype: list
        """
        all_reviews = []
        # parse first page separately because need to get pages count
        first_review_page_response = self.downloader.get(self.review_pattern.format(product_id, 1))
        if not first_review_page_response:
            logging.info('No first review page')
            return []
        first_review_page_soup = BeautifulSoup(first_review_page_response, 'lxml')
        first_page_reviews_raw = self.get_single_page_reviews(first_review_page_soup)
        # parse reviews on page
        first_page_reviews = list(map(self.parse_single_review, first_page_reviews_raw))
        all_reviews.extend(first_page_reviews)
        pages_count = DresslilyScraper.get_pages_count(first_review_page_soup, True)
        if not pages_count:
            # Only reviews < 6
            return all_reviews
        # getting pages links
        pages_links = list(map(lambda page: self.review_pattern.format(product_id, page), range(2, pages_count + 1)))
        # parse all pages
        pool_result = list(map(self.scrape_review_page, pages_links))
        scraped_reviews = reduce(lambda first, second: first + second, pool_result)
        all_reviews.extend(scraped_reviews)
        return all_reviews

    def scrape_review_page(self, link):
        """
        Making request to review page link, get all reviews and parse them
        :param link: link to review page
        :type link: str
        :return: list of one page parsed reviews
        :rtype: list
        """
        response = self.downloader.get(link)
        soup = BeautifulSoup(response, 'lxml')
        reviews = self.get_single_page_reviews(soup)
        parsed_reviews = [review for review in map(self.parse_single_review, reviews) if review]
        return parsed_reviews

    @staticmethod
    def get_single_page_reviews(review_page_soup):
        """
        Getting all reviews on review page
        :param review_page_soup: review page soup
        :type review_page_soup: BeautifulSoup
        :return: list of reviews on this page
        :rtype: list
        """
        single_page_reviews = review_page_soup.find_all('div', class_='reviewlist clearfix')
        return single_page_reviews

    def parse_single_review(self, single_review_soup):
        """
        Parse all data from single review
        :param single_review_soup: single review soup object
        :type single_review_soup: BeautifulSoup
        :return: review primary attributes
        :rtype: dict
        """
        review_info = dict()
        review_info['rating'] = self.get_review_rating(single_review_soup)
        review_info['timestamp'] = self.get_review_timestamp(single_review_soup)
        review_info['text'] = self.get_review_text(single_review_soup)
        review_info['size'] = self.get_review_size(single_review_soup)
        review_info['color'] = self.get_review_color(single_review_soup)
        return review_info

    @staticmethod
    def get_review_rating(single_review_soup):
        """
        Count how many stars review has
        :param single_review_soup: single review soup
        :type single_review_soup: BeautifulSoup
        :return: list of stars length
        :rtype: int
        """
        rating = len(single_review_soup.find_all('i', class_='icon-star-black'))
        return rating

    @staticmethod
    def get_review_timestamp(single_review_soup):
        """
        Converts publication time string in datetime object and then get timestamp from it
        :type single_review_soup: BeautifulSoup
        :return: review publication timestamp
        :rtype: float
        """
        time_string = single_review_soup.find('span', class_='reviewtime').text
        review_timestamp = datetime.datetime.strptime(time_string, '%b,%d %Y %H:%M:%S').timestamp()
        return review_timestamp

    @staticmethod
    def get_review_text(single_review_soup):
        """
        :type single_review_soup: BeautifulSoup
        :rtype: str
        """
        review_text = single_review_soup.find('p', class_='reviewcon').text
        return review_text

    @staticmethod
    def get_review_size(single_review_soup):
        """
        :type single_review_soup: BeautifulSoup
        :rtype: str
        """
        try:
            review_size = single_review_soup.find('span', text=re.compile('^Size:')).text.replace('Size:', '').strip()
        except AttributeError:
            # case with no size
            logging.warning('Cant get review size')
            review_size = None
        return review_size

    @staticmethod
    def get_review_color(single_review_soup):
        """
        :type single_review_soup: BeautifulSoup
        :rtype: str
        """
        try:
            review_color = single_review_soup.find('span', text=re.compile('^Color:')).text.replace('Color:',
                                                                                                    '').strip()
        except AttributeError:
            # case with no color
            logging.warning('Cant get review color')
            review_color = None
        return review_color
