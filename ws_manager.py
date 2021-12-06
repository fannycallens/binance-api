import asyncio
import gzip
import json
import logging
from asyncio import sleep
from random import random
from socket import gaierror
import websockets as ws
from typing import Optional, List, Dict, Callable, Any
from websockets.exceptions import ConnectionClosedError
from api_client import AsyncClient

KEEPALIVE_TIMEOUT = 5 * 60  # 5 minutes


class ReconnectingWebsocket:
    MAX_RECONNECTS = 5
    MAX_RECONNECT_SECONDS = 60
    MIN_RECONNECT_WAIT = 0.1
    TIMEOUT = 10
    NO_MESSAGE_RECONNECT_TIMEOUT = 60
    MAX_QUEUE_SIZE = 100

    def __init__(
        self, loop, url: str, path: Optional[str] = None, prefix: str = 'ws/', is_binary: bool = False, exit_coro=None
    ):
        self._loop = loop or asyncio.get_event_loop()
        self._log = logging.getLogger(__name__)
        self._path = path
        self._url = url
        self._exit_coro = exit_coro
        self._prefix = prefix
        self._reconnects = 0
        self._is_binary = is_binary
        self._conn = None
        self._socket = None
        self.ws: Optional[ws.WebSocketClientProtocol] = None
        self.ws_state = 'initialising'
        self._queue = asyncio.Queue(loop=self._loop)
        self._handle_read_loop = None

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._exit_coro:
            await self._exit_coro()
        self.ws_state = 'exiting'
        if self.ws:
            self.ws.fail_connection()
        if self._conn and hasattr(self._conn, 'protocol'):
            await self._conn.__aexit__(exc_type, exc_val, exc_tb)
        self.ws = None
        if not self._handle_read_loop:
            self._log.error("CANCEL read_loop")
            await self._kill_read_loop()

    async def connect(self):
        await self._before_connect()
        self.ws_state = 'streaming'
        ws_url = self._url + self._prefix + self._path
        self._conn = ws.connect(ws_url, close_timeout=0.1)
        try:
            self.ws = await self._conn.__aenter__()
        except:
            await self._reconnect()
            return
        self._reconnects = 0
        await self._after_connect()
        # To manage the "cannot call recv while another coroutine is already waiting for the next message"
        if not self._handle_read_loop:
            self._handle_read_loop = self._loop.call_soon_threadsafe(asyncio.create_task, self._read_loop())

    async def _kill_read_loop(self):
        self.ws_state = 'exiting'
        while self._handle_read_loop:
            await sleep(0.1)

    async def _before_connect(self):
        pass

    async def _after_connect(self):
        pass

    def _handle_message(self, evt):
        if self._is_binary:
            try:
                evt = gzip.decompress(evt)
            except (ValueError, OSError):
                return None
        try:
            return json.loads(evt)
        except ValueError:
            self._log.debug(f'error parsing evt json:{evt}')
            return None

    async def _read_loop(self):
        try:
            while True:
                try:
                    if self.ws_state == 'reconnecting':
                        await self._run_reconnect()

                    if not self.ws or self.ws_state != 'streaming':
                        await self._wait_for_reconnect()
                        break
                    elif self.ws_state == 'exiting':
                        break
                    elif self.ws.state == ws.protocol.State.CLOSING:
                        await asyncio.sleep(0.1)
                        continue
                    elif self.ws.state == ws.protocol.State.CLOSED:
                        await self._reconnect()
                    elif self.ws_state == 'streaming':
                        res = await asyncio.wait_for(self.ws.recv(), timeout=self.TIMEOUT)
                        #print(res)
                        res = self._handle_message(res)
                        if res:
                            if self._queue.qsize() < self.MAX_QUEUE_SIZE:
                                await self._queue.put(res)
                            else:
                                self._log.debug(f"Queue overflow {self.MAX_QUEUE_SIZE}. Message not filled")
                                await self._queue.put({
                                    'e': 'error',
                                    'm': 'Queue overflow. Message not filled'
                                })
                                raise Exception
                except asyncio.TimeoutError:
                    self._log.debug(f"no message in {self.TIMEOUT} seconds")
                    # _no_message_received_reconnect
                except asyncio.CancelledError as e:
                    self._log.debug(f"cancelled error {e}")
                    break
                except asyncio.IncompleteReadError as e:
                    self._log.debug(f"incomplete read error ({e})")
                except ConnectionClosedError as e:
                    self._log.debug(f"connection close error ({e})")
                except gaierror as e:
                    self._log.debug(f"DNS Error ({e})")
                except Exception as e:
                    self._log.debug(f"BinanceWebsocketUnableToConnect ({e})")
                    break
                except Exception as e:
                    self._log.debug(f"Unknown exception ({e})")
                    continue
        finally:
            self._handle_read_loop = None  #

            # al the coro is stopped
            self._reconnects = 0

    async def _run_reconnect(self):
        await self.before_reconnect()
        if self._reconnects < self.MAX_RECONNECTS:
            reconnect_wait = self._get_reconnect_wait(self._reconnects)
            self._log.debug(
                f"websocket reconnecting. {self.MAX_RECONNECTS - self._reconnects} reconnects left - "
                f"waiting {reconnect_wait}"
            )
            await asyncio.sleep(reconnect_wait)
            await self.connect()
        else:
            self._log.error(f'Max reconnections {self.MAX_RECONNECTS} reached:')
            # Signal the error
            await self._queue.put({
                'e': 'error',
                'm': 'Max reconnect retries reached'
            })
            raise Exception

    async def recv(self):
        res = None
        while not res:
            try:
                res = await asyncio.wait_for(self._queue.get(), timeout=self.TIMEOUT)
            except asyncio.TimeoutError:
                self._log.debug(f"no message in {self.TIMEOUT} seconds")
        return res

    async def _wait_for_reconnect(self):
        while self.ws_state != 'streaming':
            await sleep(0.1)

    def _get_reconnect_wait(self, attempts: int) -> int:
        expo = 2 ** attempts
        return round(random() * min(self.MAX_RECONNECT_SECONDS, expo - 1) + 1)

    async def before_reconnect(self):
        if self.ws:
            await self._conn.__aexit__(None, None, None)
            self.ws = None
        self._reconnects += 1

    def _no_message_received_reconnect(self):
        self._log.debug('No message received, reconnecting')
        self.ws_state = 'reconnecting'

    async def _reconnect(self):
        self.ws_state = 'reconnecting'


class SocketManager:
    STREAM_URL = 'wss://stream.binance.{}:9443/'
    STREAM_TESTNET_URL = 'wss://testnet.binance.vision/'
    WEBSOCKET_DEPTH_5 = '5'

    def __init__(self, client: AsyncClient, loop=None, user_timeout=KEEPALIVE_TIMEOUT):
        self.STREAM_URL = self.STREAM_URL.format(client.tld)
        self._conns = {}
        self._loop = loop or asyncio.get_event_loop()
        self._client = client
        self._user_timeout = user_timeout
        self.testnet = self._client.testnet

    def _get_stream_url(self, stream_url: [str] = None):
        if stream_url:
            return stream_url
        stream_url = self.STREAM_URL
        if self.testnet:
            stream_url = self.STREAM_TESTNET_URL
        return stream_url

    def _get_socket(
        self, path: str, stream_url: [str] = None, prefix: str = 'ws/', is_binary: bool = False
    ) -> str:
        conn_id = path #bnbbtc@depth5
        if conn_id not in self._conns:
            self._conns[conn_id] = ReconnectingWebsocket(
                loop=self._loop,
                path=path,
                url=self._get_stream_url(stream_url),
                prefix=prefix,
                exit_coro=self._exit_socket,
                is_binary=is_binary
            )

        return self._conns[conn_id]

    async def _exit_socket(self, path: str):
        await self._stop_socket(path)

    def depth_socket(self, symbol: str, depth: [str] = None):
        """Start a websocket for symbol market depth returning either a diff or a partial book

        https://github.com/binance-exchange/binance-official-api-docs/blob/master/web-socket-streams.md#partial-book-depth-streams

        :param symbol: required
        :type symbol: str
        :param depth: optional Number of depth entries to return, default None. If passed returns a partial book instead of a diff
        :type depth: str
        :param interval: optional interval for updates, default None. If not set, updates happen every second. Must be 0, None (1s) or 100 (100ms)
        :type interval: int

        :returns: connection key string if successful, False otherwise

        Partial Message Format

        .. code-block:: python

            {
                "lastUpdateId": 160,  # Last update ID
                "bids": [             # Bids to be updated
                    [
                        "0.0024",     # price level to be updated
                        "10",         # quantity
                        []            # ignore
                    ]
                ],
                "asks": [             # Asks to be updated
                    [
                        "0.0026",     # price level to be updated
                        "100",        # quantity
                        []            # ignore
                    ]
                ]
            }


        Diff Message Format

        .. code-block:: python

            {
                "e": "depthUpdate", # Event type
                "E": 123456789,     # Event time
                "s": "BNBBTC",      # Symbol
                "U": 157,           # First update ID in event
                "u": 160,           # Final update ID in event
                "b": [              # Bids to be updated
                    [
                        "0.0024",   # price level to be updated
                        "10",       # quantity
                        []          # ignore
                    ]
                ],
                "a": [              # Asks to be updated
                    [
                        "0.0026",   # price level to be updated
                        "100",      # quantity
                        []          # ignore
                    ]
                ]
            }

        """
        socket_name = symbol.lower() + '@depth'
        if depth and depth != '1':
            socket_name = f'{socket_name}{depth}'
        conn = self._get_socket(socket_name)
        import json
        subscribe_event = {
        "method": "SUBSCRIBE",
        "params": [socket_name],
        "id": 1
        }
        json_subscribe_event = json.dumps(subscribe_event)
        #self._conns.send(json_subscribe_event)
        print(socket_name) #bnbbtc@depth5

        #conn._socket.send(json_subscribe_event)
        #print(dir(conns.ws))
        return self._get_socket(socket_name)


    async def _stop_socket(self, conn_key):
        if conn_key not in self._conns:
            return
        del (self._conns[conn_key])



