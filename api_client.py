from typing import Dict, Optional, List, Tuple
import hashlib
import hmac
import requests
import time
import asyncio
import aiohttp
from operator import itemgetter


class BaseClient:

    API_URL = 'https://api.binance.{}/api'
    API_TESTNET_URL = 'https://testnet.binance.vision/api'
    PUBLIC_API_VERSION = 'v1'
    PRIVATE_API_VERSION = 'v3'
    REQUEST_TIMEOUT: float = 10

    SYMBOL_TYPE_SPOT = 'SPOT'

    def __init__(
        self, api_key: Optional[str] = None, api_secret: Optional[str] = None,
        requests_params: Dict[str, str] = None, tld: str = 'com',
        testnet: bool = False
    ):

        self.tld = tld
        self.API_URL = self.API_URL.format(tld)
        self.API_KEY = api_key
        self.API_SECRET = api_secret
        self.session = self._init_session()
        self._requests_params = requests_params
        self.response = None
        self.testnet = testnet
        self.timestamp_offset = 0

    def _get_headers(self) -> Dict:
        headers = {
            'Accept': 'application/json',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36',

        }
        if self.API_KEY:
            assert self.API_KEY
            headers['X-MBX-APIKEY'] = self.API_KEY
        return headers

    def _init_session(self):
        raise NotImplementedError

    def _create_api_uri(self, path: str, version: str = 'PUBLIC_API_VERSION', **kwargs) -> str:
        url = self.API_URL
        if self.testnet:
            url = self.API_TESTNET_URL
        final_url = url + '/' + version + '/' + path
        if 'symbol' in kwargs and 'limit' in kwargs:
            # https://api.binance.com/api/v3/depth?symbol=BNBBTC&limit=1000
            final_url += '?symbol=' + str(kwargs['symbol']) + '&' + 'limit=' + str(kwargs['limit'])
        return final_url

    def _generate_signature(self, data: Dict) -> str:
        ordered_data = self._order_params(data)
        query_string = '&'.join([f"{d[0]}={d[1]}" for d in ordered_data])
        m = hmac.new(self.API_SECRET.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256)
        return m.hexdigest()

    @staticmethod
    def _order_params(data: Dict) -> List[Tuple[str, str]]:
        data = dict(filter(lambda el: el[1] is not None, data.items()))
        has_signature = False
        params = []
        for key, value in data.items():
            if key == 'signature':
                has_signature = True
            else:
                params.append((key, str(value)))
        # sort parameters by key
        params.sort(key=itemgetter(0))
        if has_signature:
            params.append(('signature', data['signature']))
        return params

    def _get_request_kwargs(self, method, signed: bool, force_params: bool = False, **kwargs) -> Dict:
        # set default requests timeout
        kwargs['timeout'] = self.REQUEST_TIMEOUT
        if signed:
            # generate signature
            kwargs['data']['timestamp'] = int(time.time() * 1000 + self.timestamp_offset)
            kwargs['data']['signature'] = self._generate_signature(kwargs['data'])

        return kwargs

class Client(BaseClient):

    def __init__(
            self, api_key: Optional[str] = None, api_secret: Optional[str] = None,
            requests_params: Dict[str, str] = None, tld: str = 'com',
            testnet: bool = False
    ):

        super().__init__(api_key, api_secret, requests_params, tld, testnet)

        # init DNS and SSL cert
        self.ping()

    def _init_session(self) -> requests.Session:
        headers = self._get_headers()
        session = requests.session()
        session.headers.update(headers)
        return session

    def _request(self, method, uri: str, signed: bool, force_params: bool = False, **kwargs):
        kwargs = self._get_request_kwargs(method, signed, force_params, **kwargs)
        self.response = getattr(self.session, method)(uri, **kwargs)
        return self._handle_response(self.response)

    @staticmethod
    def _handle_response(response: requests.Response):
        if not (200 <= response.status_code < 300):
            raise Exception(response, response.status_code, response.text)
        try:
            return response.json()
        except ValueError:
            raise Exception('Invalid Response: %s' % response.text)

    def _request_api(
            self, method, path: str, signed: bool = False, version=BaseClient.PUBLIC_API_VERSION, **kwargs
    ):
        uri = self._create_api_uri(path, signed, version, **kwargs)
        return self._request(method, uri, signed, **kwargs)

    def ping(self) -> Dict:
        return self._get('ping', version=self.PING_API_VERSION)


    def get_order_book(self, **params) -> Dict:
        return self._get('depth', data=params, version=self.PUBLIC_API_VERSION)

    def close_connection(self):
        if self.session:
            self.session.close()

    def __del__(self):
        self.close_connection()

class AsyncClient(BaseClient):

    def __init__(
        self, api_key: Optional[str] = None, api_secret: Optional[str] = None,
        requests_params: Dict[str, str] = None, tld: str = 'com',
        testnet: bool = False, loop=None
    ):

        self.loop = loop or asyncio.get_event_loop()
        super().__init__(api_key, api_secret, requests_params, tld, testnet)

    @classmethod
    async def create(
        cls, api_key: Optional[str] = None, api_secret: Optional[str] = None,
        requests_params: Dict[str, str] = None, tld: str = 'com',
        testnet: bool = False, loop=None
    ):

        self = cls(api_key, api_secret, requests_params, tld, testnet, loop)

        try:
            await self.ping()
            return self
        except Exception:
            # If ping throw an exception, the current self must be cleaned
            # else, we can receive a "asyncio:Unclosed client session"
            await self.close_connection()
            raise

    def _init_session(self) -> aiohttp.ClientSession:

        session = aiohttp.ClientSession(
            loop=self.loop,
            headers=self._get_headers()
        )
        return session

    async def close_connection(self):
        if self.session:
            assert self.session
            await self.session.close()

    async def _request(self, method, uri: str, signed: bool, force_params: bool = False, **kwargs):
        #add timeout param to request : {'timeout': 10} and signature
        kwargs = self._get_request_kwargs(method, signed, force_params, **kwargs)

        async with getattr(self.session, method)(uri, **kwargs) as response:
            self.response = response
            return await self._handle_response(response)

    async def _handle_response(self, response: aiohttp.ClientResponse):
        if not str(response.status).startswith('2'):
            raise Exception(response, response.status, await response.text())
        try:
            return await response.json()
        except ValueError:
            txt = await response.text()
            raise Exception(f'Invalid Response: {txt}')

    async def _request_api(self, method, path, version=BaseClient.PUBLIC_API_VERSION, **kwargs):
        uri = self._create_api_uri(path, version, **kwargs)
        return await self._request(method, uri, False)

    async def _get(self, path, version=BaseClient.PUBLIC_API_VERSION, **kwargs):
        return await self._request_api('get', path, version, **kwargs)

    async def ping(self) :
        return await self._get('ping', version=self.PUBLIC_API_VERSION)

    async def get_order_book(self, **params) -> Dict:
        #params = {'symbol': 'BTCBUSD', 'limit': 500}
        return await self._get('depth', version=self.PRIVATE_API_VERSION, **params)