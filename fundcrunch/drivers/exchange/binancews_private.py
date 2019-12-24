import queue, threading
from  multiprocessing import Process
from time import sleep
import datetime
import time
import sys, json
import zmq
import asyncio
import websockets
import itertools
import urllib.request, requests
import operator
import math, hashlib

from .exchdriver import ExchangeDriver
from .ccxtdriver import CcxtDriver

wss = 'wss://stream.binance.com:9443'
api_base = "https://api.binance.com"
API_URL = "https://api.binance.com/api"

DEFAULT_USER_TIMEOUT = 30 * 60


class BinanceWebSocketPrivateStreams(threading.Thread):
    def __init__(self, config, loop):
        threading.Thread.__init__(self)
        self.config = config
        self.pname = '[binance_ws_private]'

        self.ob_update_queue = []
        self.ob_snapshot_queue = queue.Queue()
        self.balance_local={}

        self.is_auth = False

        if 'auth' in self.config.keys():
            self.is_auth = True
            self.pname = f"[binance_ws_private {self.config['auth']['apiKey'][:6]}*]"
        
        self.apiKey = self.config['auth']['apiKey']
        self.apiSecret = self.config['auth']['secret']

        self.session = requests.session()
        self.session.headers.update({'Accept': 'application/json',
                                'User-Agent': 'binance/python',
                                'X-MBX-APIKEY': self.apiKey})

        r = self.session.post(api_base+'/api/v1/userDataStream')
        self.listenKey = r.json()['listenKey']

        self.aliveThread = threading.Thread(target=self.keep_alive)
        self.aliveThread.start()


        #loop = asyncio.new_event_loop()
        client1 = loop.run_until_complete(self.ws_listener())

        try:
            loop.run_forever()
        except KeyboardInterrupt:
            pass

        client1.close()

    def keep_alive(self):
        while True:
            time.sleep(30*60)
            r = self.session.put(api_base + '/api/v1/userDataStream', data={'listenKey': self.listenKey})



    async def ws_listener(self):
        listen_url = wss + '/ws/'+ self.listenKey

        while True:
            try:

                async with websockets.connect(uri=listen_url) as ws:

                    while True:
                        m = await ws.recv()
                        message = json.loads(m)
                        to_send = None

                        if message['e'] == 'executionReport':
                            to_send = self.build_order_update(message)
                        elif message['e'] == 'outboundAccountInfo':
                            to_send = self.build_balance_update(message)


                        if to_send:
                            if type(to_send) is dict:
                                self.config['web_socket_streams_out'].put( to_send )
                            elif type(to_send) is list:
                                for i in to_send:
                                    self.config['web_socket_streams_out'].put(i)
            except Exception as e:
                print(self.name, e)
                del ws



    def _init_session(self):
        session = requests.session()
        
    def __del__(self):

        r = self.session.delete(api_base + '/api/v1/userDataStream', data={'listenKey': 'YES'})
        print(self.pname, 'delete listenKey', r)
        self.aliveThread.join()



    def symbol_convert(self,symbol):
        # -------------------------------------
        # aligne symbnol name to normal
        # --------------------------------------
        if symbol[-3:] in ['ETH','BTC','BNB']:
            taget_len = len(symbol)-3
            symbol = symbol[:taget_len] +'/'+ symbol[taget_len:]

        elif symbol[-4:] in ['USDT']:
            taget_len = len(symbol)-4
            symbol = symbol[:taget_len] +'/'+ symbol[taget_len:]
        
        return symbol
    
    def build_order_update(self, message):
        
        message['message'] = 'order_update'
        message['exchange'] = 'binance'
        message['symbol'] = self.symbol_convert(message['s'])
        message['tiona_timestamp'] = time.time()
        message['auth'] = self.apiKey
        
        return message
    
    def build_balance_update(self, message):
        res = []
        for i in message['B']:

            is_up = False
            
            if i['a'] not in self.balance_local.keys():
                is_up = True

                self.balance_local[i['a']] = {'free':float(i['f']),
                                              'used':float(i['l']), 
                                              'total': float(i['f'])+float(i['l']) }
                                    
            
            elif self.balance_local[i['a']]['free'] != float(i['f']) or self.balance_local[i['a']]['used'] != float(i['l']):
                is_up = True                
                self.balance_local[i['a']] = {'free':float(i['f']),
                                              'used':float(i['l']), 
                                              'total': float(i['f'])+float(i['l']) }


            if is_up:
                res.append( {'message': 'balance',
                            'exchange': 'binance',
                            'symbol': i['a'],
                            'tiona_timestamp': time.time(),
                            'auth': self.apiKey,
                            'balance': self.balance_local[i['a']]
                            } )

        return res
    
