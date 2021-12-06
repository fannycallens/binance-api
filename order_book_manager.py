'''
How to manage a local order book correctly
Open a stream to wss://stream.binance.com:9443/ws/bnbbtc@depth.
Buffer the events you receive from the stream.
Get a depth snapshot from https://api.binance.com/api/v3/depth?symbol=BNBBTC&limit=1000 .
Drop any event where u is <= lastUpdateId in the snapshot.
The first processed event should have U <= lastUpdateId+1 AND u >= lastUpdateId+1.
While listening to the stream, each new event's U should be equal to the previous event's u+1.
The data in each event is the absolute quantity for a price level.
If the quantity is 0, remove the price level.
Receiving an event that removes a price level that is not in your local order book can happen and is normal.

'''

import logging
from operator import itemgetter
import asyncio
import time
import ws_manager


class DepthCache(object):

    def __init__(self, symbol, conv_type=float):

        self.symbol = symbol
        self._bids = {}
        self._asks = {}
        self.update_time = None
        self.conv_type = conv_type
        self._log = logging.getLogger(__name__)

    def add_bid(self, bid):

        self._bids[bid[0]] = self.conv_type(bid[1])
        if bid[1] == "0.00000000":
            del self._bids[bid[0]]

    def add_ask(self, ask):

        self._asks[ask[0]] = self.conv_type(ask[1])
        if ask[1] == "0.00000000":
            del self._asks[ask[0]]

    def get_bids(self):

        return DepthCache.sort_depth(self._bids, reverse=True, conv_type=self.conv_type)

    def get_asks(self):

        return DepthCache.sort_depth(self._asks, reverse=False, conv_type=self.conv_type)

    @staticmethod
    def sort_depth(vals, reverse=False, conv_type=float):
        #Sort bids or asks by price
        if isinstance(vals, dict):
            lst = [[conv_type(price), conv_type(quantity)] for price, quantity in vals.items()]
        elif isinstance(vals, list):
            lst = [[conv_type(price), conv_type(quantity)] for price, quantity in vals]
        else:
            raise ValueError(f'Unknown order book depth data type: {type(vals)}')
        lst = sorted(lst, key=itemgetter(0), reverse=reverse)
        return lst


class BaseDepthCacheManager:
    DEFAULT_REFRESH = 60 * 30  # 30 minutes
    TIMEOUT = 60

    def __init__(self, client, symbol, loop=None, bm=None, limit=10, conv_type=float):

        self._client = client
        self._depth_cache = None
        self._loop = loop or asyncio.get_event_loop()
        self._symbol = symbol
        self._limit = limit
        self._last_update_id = None
        self._bm = bm or ws_manager.SocketManager(self._client, self._loop)
        self._conn_key = None
        self._conv_type = conv_type
        self._log = logging.getLogger(__name__)

    async def __aenter__(self):
        await asyncio.gather(
            self._init_cache(),
            self._start_socket()
        )
        await self._socket.__aenter__()
        return self

    async def __aexit__(self, *args, **kwargs):
        await self._socket.__aexit__(*args, **kwargs)

    async def recv(self):
        dc = None
        while not dc:
            try:
                res = await asyncio.wait_for(self._socket.recv(), timeout=self.TIMEOUT)
                print(res)
            except Exception as e:
                print(e)
                self._log.warning(e)
            else:
                dc = await self._depth_event(res)
        return dc

    async def _init_cache(self):

        # initialise or clear depth cache
        self._depth_cache = DepthCache(self._symbol, conv_type=self._conv_type)

    async def _start_socket(self):

        self._socket = self._get_socket()
        await self._socket.connect()

        # import json
        # subscribe_event = {
        #     "method": "SUBSCRIBE",
        #     "params": [self._socket._path],
        #     "id": 1
        # }
        # json_subscribe_event = json.dumps(subscribe_event)
        #self._socket.send(json_subscribe_event)

    def _get_socket(self):
        raise NotImplementedError

    async def _depth_event(self, msg):

        if not msg:
            return None

        if 'e' in msg and msg['e'] == 'error':
            # close the socket
            await self.close()

            # notify the user by returning a None value
            return None

        return await self._process_depth_message(msg)

    async def _process_depth_message(self, msg):

        # add any bid or ask values
        self._apply_orders(msg)

        # call the callback with the updated depth cache
        res = self._depth_cache

        # after processing event see if we need to refresh the depth cache
        if self._refresh_interval and int(time.time()) > self._refresh_time:
            await self._init_cache()

        return res

    def _apply_orders(self, msg):
        for bid in msg.get('b', []) + msg.get('bids', []):
            self._depth_cache.add_bid(bid)
        for ask in msg.get('a', []) + msg.get('asks', []):
            self._depth_cache.add_ask(ask)

        # keeping update time
        self._depth_cache.update_time = msg.get('E') or msg.get('lastUpdateId')

    def get_depth_cache(self):

        return self._depth_cache

    async def close(self):

        self._depth_cache = None

    def get_symbol(self):

        return self._symbol


class DepthCacheManager(BaseDepthCacheManager):

    def __init__(
        self, client, symbol, loop=None, bm=None, limit=500, conv_type=float, ws_interval=None
    ):
        super().__init__(client, symbol, loop, bm, limit, conv_type)


    async def _init_cache(self):
        # Initialise the depth cache calling REST endpoint

        self._last_update_id = None
        self._depth_message_buffer = []

        res = await self._client.get_order_book(symbol=self._symbol, limit=self._limit)

        # initialise or clear depth cache
        await super()._init_cache()

        # process bid and asks from the order book
        self._apply_orders(res)
        if 'bids' in res :
            for bid in res['bids']:
                self._depth_cache.add_bid(bid)
        if 'asks' in res :
            for ask in res['asks']:
                self._depth_cache.add_ask(ask)

        # set first update id
        if 'lastUpdateId' in res:
            self._last_update_id = res['lastUpdateId']

        # Apply any updates from the websocket
        for msg in self._depth_message_buffer:
            await self._process_depth_message(msg)

        # clear the depth buffer
        self._depth_message_buffer = []

    async def _start_socket(self):
        if not getattr(self, '_depth_message_buffer', None):
            self._depth_message_buffer = []

        await super()._start_socket()

    def _get_socket(self):
        return self._bm.depth_socket(self._symbol, depth=5)

    async def _process_depth_message(self, msg):

        if self._last_update_id is None:
            # Initial depth snapshot fetch not yet performed, buffer messages
            self._depth_message_buffer.append(msg)
            return

        if 'u' in msg and msg['u'] <= self._last_update_id:
            # ignore any updates before the initial update id
            return
        elif 'U' in msg and msg['U'] != self._last_update_id + 1:
            # if not buffered check we get sequential updates
            # otherwise init cache again
            await self._init_cache()

        # add any bid or ask values
        self._apply_orders(msg)

        # call the callback with the updated depth cache
        res = self._depth_cache
        if 'u' in msg :
            self._last_update_id = msg['u']

        return res
