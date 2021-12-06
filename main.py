import os
import api_client
import ws_manager
import order_book_manager
import asyncio



async def main(loop):
    api_key = os.environ.get('api_key')
    #print(api_key)
    api_secret = os.environ.get('api_key')

    #print(api_secret)
    client = await api_client.AsyncClient.create(api_key, api_secret)
    #client = await api_client.AsyncClient.create(testnet=True)
    #client.API_URL = 'https://testnet.binance.vision/api'
    #client = await api_client.AsyncClient.create()
    #obm_socket = order_book_manager.DepthCacheManager(client, 'BNBBTC', loop)
    obm_socket = order_book_manager.DepthCacheManager(client, 'BTCBUSD', loop)

    #print(dir(obm_socket))

    async with obm_socket:
        while True:
            depth_cache = await obm_socket.recv()
            print("symbol {}".format(depth_cache.symbol))
            print("top 5 bids")
            print(depth_cache.get_bids()[:5])
            print("top 5 asks")
            print(depth_cache.get_asks()[:5])
            #print("last update time {}".format(depth_cache.update_time))


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

