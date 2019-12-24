import threading, queue
import datetime
import decimal
import pprint
import json
import sys, getopt, os
from time import sleep
import requests
import logging
from requests.auth import HTTPBasicAuth

import asyncio
import websockets
import websocket

try:
    import thread
except ImportError:
    import _thread as thread
import time


from settings import latoken_cookie

logger = logging.getLogger('LAToken Driver')
hdlr = logging.FileHandler('latoken_driver.log')
ch = logging.StreamHandler()

formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s %(pair)s %(error)s')
hdlr.setFormatter(formatter)
ch.setFormatter(formatter)

logger.addHandler(hdlr)
logger.addHandler(ch)

logger.setLevel(logging.WARNING)
#ch.setLevel(logging.WARNING)


ENDPOINT_URL = 'https://api.latoken.com/'
ENDPOINT_URLWS = 'ws://api.latoken.com/'

TIMEOUT = 5  # seconds
TICK_TIMER = 5
pp = pprint.PrettyPrinter(indent=4)


# asyncio.get_event_loop().run_until_complete(
#                         hello('wss://api.latoken.com/socket.io/?EIO=3&transport=websocket'))


# asyncio.get_event_loop().run_until_complete( websockets.serve(echo, 'localhost', 8765))

class LAWS(threading.Thread):
    def __init__(self, loop, out_queues, cookie):

        self.pname = '[la_ws %s*]'%cookie[:6]
        self.queues = out_queues
        self.cookie = cookie


        #loop = asyncio.new_event_loop()
        client = loop.run_until_complete(self.get_asset())

        try:
            loop.run_forever()
        except KeyboardInterrupt:
            pass

        client.close()
        loop.run_until_complete(server.wait_closed())
        loop.close()

    def parse_tiker(self, tick):
        res = None
        if tick[:2] == '42':
            #print(tick[2:])
            js = json.loads(tick[2:])

            #update_45 dict_keys(['buyOrders', 'allOrdersHistory', 'sellOrders', 'prevDayPrice', 'lastPrice', 'high24h', 'low24h'])
            #update_45 dict_keys(['balances', 'ordersHistory', 'position', 'myOpenOrders'])

            f, pair_id = js[0].split('_')
            #print(pair_id ,js[1].keys())

            if f=='update':


                res=dict()
                res['account_name'] = self.cookie[:6]+'8'

                if 'balances' in js[1].keys():
                    res['type'] = 'private'
                    res['balance'] = js[1]['balances']
                    res['orders'] = js[1]['userOrders']

                    for i in js[1]['filledOrders']:
                        i['orderId'] = i['id']
                        i['amount'] = i['total']

                    #print(js[1]['filledOrders'])

                    res['account_executed_orders'] = js[1]['filledOrders']
                    print(datetime.datetime.now().strftime('%H:%M:%S'),self.pname, int(pair_id), 'get private data' )

                elif 'high24h' in js[1].keys():
                    res['type'] = 'public'
                    res['sell'] = js[1]['sellOrders']
                    res['buy'] = js[1]['buyOrders']
                    print(datetime.datetime.now().strftime('%H:%M:%S'),self.pname, int(pair_id), 'get public data' )


                # res['asset_timing'] = tick['timing']
                # res['counts'] = tick['counts']
                # res['account_name'] = self.account_name
                # res['balance'] = tick['data']['balances']
                # res['sell'] = tick['data']['sellOrders']
                # res['buy'] = tick['data']['buyOrders']
                # res['orders'] = tick['data']['myOpenOrders']
                # res['account_executed_orders'] = tick['data']['ordersHistory']

                self.out_queues.put(res)

        return res


    async def get_asset(self):

        print(self.pname, 'initialized!')

        while True:
            try:
                async with websockets.connect(uri = 'wss://api.latoken.com/socket.io/?EIO=3&transport=websocket',
                                              extra_headers = {'cookie': 'x-zalogo-id=%s;'%self.cookie } ) as ws:
                    while True:
                        m = await ws.recv()
                        self.parse_tiker(m)
                        #jd = json.loads(m)
                        #print(m)

            except websockets.exceptions.ConnectionClosed as e:
                print(e, id(ws))
                del ws
