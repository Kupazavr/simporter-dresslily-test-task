import requests
from helpers.proxy_helper import ProxyHelper, BadProxyError
import logging
import copy
import time


class Downloader:
    def __init__(self, check_url, use_proxy=True, attempts=20, use_user_agents=True, use_session=False, request_per_min=20):
        self.check_url = check_url
        self.use_proxy = use_proxy
        self.use_session = use_session
        self.update_request_maker()
        self.proxy_helper = ProxyHelper(check_url, use_proxy, request_per_min)
        self.attempts = attempts
        self.use_user_agents = use_user_agents
        self.user_agents_list = ProxyHelper.load_user_agents()
        self.proxy_auth = {}
        self.session_update_time = time.time()

    def update_request_maker(self):
        if self.use_session:
            self.request_maker = requests.Session()
            # raise max pool size for use bigger thread pool size
            a = requests.adapters.HTTPAdapter(pool_maxsize=9999)
            self.request_maker.mount('https://', a)
        else:
            self.request_maker = requests

    def get_proxies(self, proxy):
        """
        Creating proxy object from simple proxy string
        :param proxy: proxy string with {ip}:{port} pattern
        :type proxy: str
        :return: proxy object
        :rtype: dict
        """
        # get login and password if proxy required logining
        login = self.proxy_auth.get(proxy, {}).get('login', '')
        password = self.proxy_auth.get(proxy, {}).get('password', '')
        proxy_string = '{}' + f'://{login}:{password}@{proxy}'
        proxies = {'https': proxy_string.format('https'),
                   'http': proxy_string.format('http')}
        return proxies

    def create_request(self, method, url, params={}, cookies=None, data={}, headers={}, timeout=60, files=None):
        # Decorator must use instant function because we must change instant proxy dataframe only
        @self.proxy_helper.exception_decorator
        def request_to_page(proxy, **kwargs):
            """
            Request wrapper
            :param proxy: proxy string with {ip}:{port} pattern
            :type proxy: str
            :param kwargs: request params
            :return: response text
            :rtype: str
            """
            proxies = self.get_proxies(proxy) if proxy else {}
            response_text = ''
            start_time = time.time()
            response = self.request_maker.request(proxies=proxies, stream=True, **kwargs)
            for content in response.iter_content(1024, decode_unicode=True):
                if time.time() - start_time > 30:
                    # if request time longer than 30 sec must stop request
                    raise BadProxyError
                try:
                    # if content is json object, concat it raw, else decode in utf-8
                    response_text += content
                except TypeError:
                    response_text += content.decode('utf-8')
                if "You don't have permission to access" in response_text:
                    # proxy banned
                    raise BadProxyError
            return response_text
        headers = copy.deepcopy(headers)
        attempts = self.attempts
        while attempts > 0:
            if self.use_user_agents:
                random_agent = self.proxy_helper.get_random_user_agent()
                headers.update({'user-agent': random_agent})

            attempts -= 1
            # try without proxies last time
            raw_proxy = self.proxy_helper.get_proxy() if self.use_proxy and attempts != 1 else None
            try:
                request_response = request_to_page(proxy=raw_proxy,
                                                   method=method,
                                                   url=url,
                                                   params=params,
                                                   cookies=cookies,
                                                   data=data,
                                                   headers=headers,
                                                   timeout=timeout,
                                                   files=files)

                return request_response
            except Exception as e:
                logging.debug('received {} exception on request on {} try on {} link'.format(e, self.attempts - attempts, url))

    def get(self, url, params={}, cookies=None, headers={}, timeout=60):
        """
        Get method wrapper
        :param: request params
        :return: response text or None if we have no response
        :rtype: str, None
        """
        return self.create_request(method='GET',
                                   url=url,
                                   params=params,
                                   cookies=cookies,
                                   headers=headers,
                                   timeout=timeout)

    def post(self, url, params={}, cookies=None, data={}, headers={}, timeout=60, files=None):
        """
        Post method wrapper
        :param: request params
        :return: response text or None if we have no response
        :rtype: str, None
        """
        return self.create_request(method='POST',
                                   url=url,
                                   params=params,
                                   cookies=cookies,
                                   data=data,
                                   headers=headers,
                                   timeout=timeout,
                                   files=files)


