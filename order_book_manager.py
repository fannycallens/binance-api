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
import ws_manager


class OrderBook(object):

    def __init__(self, symbol):
        self.symbol = symbol
        self._bids = {}
        self._asks = {}
        self.update_time = None
        self._log = logging.getLogger(__name__)

    def add_bid(self, bid):
        self._bids[bid[0]] = float(bid[1])
        if bid[1] == "0.00000000":
            del self._bids[bid[0]]

    def add_ask(self, ask):
        self._asks[ask[0]] = float(ask[1])
        if ask[1] == "0.00000000":
            del self._asks[ask[0]]

    def get_bids(self):
        # Sort bids in descending order
        return OrderBook.sort_depth(self._bids, reverse=True)

    def get_asks(self):
        # Sort asks in ascending order
        return OrderBook.sort_depth(self._asks, reverse=False)

    @staticmethod
    def sort_depth(vals, reverse=False):
        #Sort bids or asks by price
        if isinstance(vals, dict):
            lst = [[float(price), float(quantity)] for price, quantity in vals.items()]
        else:
            raise ValueError(f'Unknown order book depth data type: {type(vals)}')
        lst = sorted(lst, key=itemgetter(0), reverse=reverse)
        return lst


class OrderBookManagerBase:
    TIMEOUT = 60

    def __init__(self, client, symbol, loop=None, bm=None, limit=10):
        self._client = client
        self._order_book_cache = None
        self._loop = loop or asyncio.get_event_loop()
        self._symbol = symbol
        self._limit = limit
        self._last_update_id = None
        self._ws_manager = bm or ws_manager.SocketManager(self._client, self._loop)
        self._conn_key = None
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
        self._order_book_cache = OrderBook(self._symbol)

    async def _start_socket(self):
        self._socket = self._get_socket()
        await self._socket.connect()

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
        res = self._order_book_cache

        return res

    def _apply_orders(self, msg):
        for bid in msg.get('bids', []):
            self._order_book_cache.add_bid(bid)
        for ask in msg.get('asks', []):
            self._order_book_cache.add_ask(ask)

        # keeping update time
        self._order_book_cache.update_time = msg.get('E') or msg.get('lastUpdateId')

    def get_depth_cache(self):
        return self._order_book_cache

    async def close(self):
        self._order_book_cache = None

    def get_symbol(self):
        return self._symbol


class OrderBookManager(OrderBookManagerBase):

    def __init__(
        self, client, symbol, loop=None, bm=None, limit=500
    ):
        super().__init__(client, symbol, loop, bm, limit)


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
                self._order_book_cache.add_bid(bid)
        if 'asks' in res :
            for ask in res['asks']:
                self._order_book_cache.add_ask(ask)

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
        return self._ws_manager.depth_socket(self._symbol, depth=5)

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
        res = self._order_book_cache
        if 'u' in msg :
            self._last_update_id = msg['u']

        return res
