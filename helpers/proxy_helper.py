import requests
import logging
import time
from helpers.helpers import chunkify, parse_config
from multiprocessing.pool import ThreadPool
from threading import RLock
from requests.exceptions import ProxyError, ConnectTimeout
import pandas as pd
import os
import csv
import gc
import random


class BadProxyError(Exception):
    def __init__(self):
        pass


class ProxyHelper:
    def __init__(self, check_url, use_proxy=True, request_per_min=20):
        self.user_agents_list = self.load_user_agents()
        self.request_per_min = request_per_min
        self.check_url = check_url
        self.config = parse_config('server')
        self.proxy_pool = ThreadPool(100)
        if use_proxy:
            self.get_valid_proxies()
        self.lock = RLock()

    def mark_proxy_as_failed(self, proxy):
        """
        Increase proxy error times and if it has more than 5, delete it
        :param proxy: proxy string with {ip}:{port} pattern
        :type proxy: str
        """
        logging.info(f'marking proxy {proxy}')
        self.proxies.at[proxy, 'previous_error_time'] = time.time()
        self.proxies.at[proxy, 'error_count'] += 1
        if int(self.proxies.at[proxy, 'error_count']) >= 5:
            self.delete_proxy(proxy)

    def delete_proxy(self, proxy):
        """
        Deleting proxy from proxies dataframe
        :param proxy: proxy string with {ip}:{port} pattern
        :type proxy: str
        """
        logging.info('removing proxy {}'.format(proxy))
        self.proxies = self.proxies.drop(index=proxy)
        if len(self.proxies) == 0:
            logging.info('proxy list is empty, getting new proxies')
            self.proxies = self.get_valid_proxies()

    def exception_decorator(self, func):
        """
        Decorator for error tracking
        """
        def wrapper(*args, proxy, **kwargs):
            try:
                request_start_time = time.time()
                result = func(proxy, *args, **kwargs)
                request_time = time.time() - request_start_time
                with self.lock:
                    # proxy can be deleted from dataframe by another thread, so we need to check if it in still
                    if proxy and proxy in self.proxies.index:
                        self.proxies.at[proxy, 'previous_request_time'] = request_time
                with self.lock:
                    if proxy and proxy in self.proxies.index:
                        # decrease on_work variable for limiting proxy usage for 5 threads only
                        self.proxies.at[proxy, 'on_work'] -= 1
                return result
            except Exception as e:
                with self.lock:
                    if proxy and proxy in self.proxies.index:
                        self.proxies.at[proxy, 'on_work'] -= 1
                        if isinstance(e, (ProxyError, ConnectTimeout, BadProxyError)):
                            # marking proxy only if it error by proxy errors
                            self.mark_proxy_as_failed(proxy)
                raise
        return wrapper

    def get_proxy(self):
        """
        Getting best free proxy
        :return valid to use proxy
        :rtype: str
        """
        attempt = 0
        while True:
            try:
                with self.lock:
                    self.sort_proxies()
                    # getting proxies only which used less than 5 threads and get error more than 1 min ago
                    mask = ((time.time() - self.proxies['previous_error_time']) > 60) & (self.proxies['on_work'] < 5)
                    chosen_proxy = list(self.proxies.loc[mask].iterrows())[0][0]
                    self.proxies.at[chosen_proxy, 'on_work'] += 1
                    break
            except IndexError:
                # if we have no valid proxy. need to wait when it gets free
                logging.debug(f"Can\'t get proxy on {attempt} retry")
                time.sleep(0.5)
                attempt += 1
        return chosen_proxy

    def sort_proxies(self):
        """Sort proxies by best response time"""
        self.proxies = self.proxies.sort_values('on_work').sort_values('previous_request_time')

    def get_valid_proxies(self):
        """
        Proxy validation check
        """
        # loads all proxies from chosen proxy service
        proxies_list = self.get_proxies_list()[:1000]
        chunks = list(chunkify(proxies_list, 500))
        valid_proxies = {}
        for n, chunk in enumerate(chunks):
            logging.info('checking {}/{} proxy batch for {}'.format(n + 1, len(chunks), self.check_url))
            # Check proxy batch
            checked_proxies = list(self.proxy_pool.map(self.check_proxy, chunk))
            gc.collect()
            # add primary proxy parameters
            valid_batch_proxies = {proxy['address']: {'previous_request_time': proxy['request_time'],
                                                      'error_count': 0,
                                                      'on_work': 0,
                                                      'previous_error_time': 0} for proxy in checked_proxies if
                                   proxy['is_valid']}
            valid_proxies.update(valid_batch_proxies)
        del checked_proxies
        # convert proxy dict into dataframe
        self.proxies = pd.DataFrame.from_dict(valid_proxies, orient='index')
        logging.info('proxy checking finished, found {} valid proxies'.format(len(self.proxies)))
        self.sort_proxies()

    @staticmethod
    def load_user_agents():
        """
                Load user agents from file
                :return: user agents list
                :rtype: list
                """
        cd = os.path.dirname(os.path.abspath(__file__))
        csvFile = os.path.join(cd, 'valid_user_agents.csv')
        with open(csvFile, 'r') as f:
            reader = csv.reader(f)
            user_agent_list = list(reader)
            user_agent_list = [x[0] for x in user_agent_list]
        return user_agent_list

    def get_random_user_agent(self):
        '''
        Get random user-agent
        :return: random user agent
        :rtype: str
        '''
        try:
            user_agent = random.choice(self.user_agents_list)
            return user_agent
        except:
            logging.error('Cannot get random user-agent', exc_info=True)
            return ''

    def check_proxy(self, proxy):
        '''
        Check proxy on response time
        :param proxy: proxy string with {ip}:{port} pattern
        :type proxy: str
        :return: proxy primary data
        :rtype: dict
        '''
        try:
            t1 = time.time()
            headers = {'user-agent': self.get_random_user_agent()}
            response = requests.get(self.check_url, proxies={
                "http": proxy,
                "https": proxy,
            }, stream=True, timeout=10, headers=headers)
            for _ in response.iter_content(1024, decode_unicode=True):
                # if request time longer than 30 sec must stop request
                if time.time() - t1 > 30:
                    raise Exception
            request_time = time.time() - t1
            return {'address': proxy, 'is_valid': True, 'request_time': request_time}
        except:
            return {'address': proxy, 'is_valid': False}

    def get_proxies_list(self):
        '''Getting proxies from used proxy service'''
        proxies_list = self.load_proxies_list(self.config['proxy_key'])
        if len(proxies_list) == 0:
            logging.error('get 0 proxies, get them from hardcoded for {}'.format(self.check_url))
            proxies_list_hardcoded = []
            proxies_list.extend(proxies_list_hardcoded)
        return proxies_list

    @staticmethod
    def load_proxies_list(key):
        '''Request to proxy service'''
        proxies_list = []
        try:
            api_url = f'https://api.best-proxies.ru/proxylist.txt?key={key}&type=http,https&limit=0'
            r = requests.get(api_url, timeout=60)
            if r.status_code == 200:
                proxies_list = r.text.split('\r\n')
        except:
            pass
        return proxies_list
