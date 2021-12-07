import os
import api_client
import ws_manager
import order_book_manager
import asyncio



async def main(loop):
    api_key = os.environ.get('api_key')
    api_secret = os.environ.get('api_key')
    client = await api_client.AsyncClient.create(api_key, api_secret)
    obm_socket = order_book_manager.OrderBookManager(client, 'BNBBTC', loop)

    async with obm_socket:
        while True:
            order_book = await obm_socket.recv()
            print("top bid entry")
            print(order_book.get_bids()[0])
            print("top ask entry")
            print(order_book.get_asks()[0])


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))

'''    
async def main():

    #api_key = os.environ.get('testnet_api_key')
    #api_secret = os.environ.get('testnet_secret_api_key')
    client = await api_client.AsyncClient.create()
    #client.API_URL = 'https://testnet.binance.vision/api'
    #set a timeout of 60 seconds
    socket_manager = ws_manager.SocketManager(client, user_timeout=60)
    ticker = 'BTCUSDT'

    my_obm = order_book_manager.DepthCacheManager(client, ticker)
    async def order_book_listener(ticker, socket_manager):

        order_book_socket = socket_manager.depth_socket(ticker)

        async with order_book_socket as order_book_stream:
            while True:
                res = await order_book_stream.recv()
                print(res)
'''

