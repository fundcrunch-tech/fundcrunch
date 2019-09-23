import queue, threading
from  multiprocessing import Process
from time import sleep
import datetime
import sys, json
import zmq
import asyncio
import websockets
import itertools
import urllib.request
import gzip
from coinmarketcap import Market

from drivers.exchdriver import ExchangeDriver

CMC_KEY = "b6b13a1b-7c12-41c2-94f2-cd50407d75b1"
CMC_TICK_TIMER = 30
CMC_PUSH_TIMER = 30

class CoinmarketcupDriver(Process, ):

    def __init__(self, addr, be, fe):
        Process.__init__(self)
        
        self.name = '[cmc]'

        self.next_cmc_update = datetime.datetime.now()
        self.next_cmc_push_timer = datetime.datetime.now()
        self.is_updated = False
       
        self.addr = addr
        self.be = be
        self.fe = fe

        self.marketcup = {}
        self.cmc_list = {}
        self.marketcup['marketcup'] = {}

        print(self.name, 'SUB/PUB', self.be, self.fe)

    def update_coin(self, coin, data):
        res = False
        
        if 'last_update' not in self.marketcup['marketcup'][coin].keys():
            self.marketcup['marketcup'][coin] = data
            res = True
        elif data['last_updated'] != self.marketcup['marketcup'][coin]['last_updated']:
            self.marketcup['marketcup'][coin] = data
            res = True

        return res    

    def run(self):
        context = zmq.Context()
        sub = context.socket(zmq.SUB)
        sub.connect(f"tcp://{self.addr}:{self.be}")
        sub.setsockopt(zmq.SUBSCRIBE, b'info')

        publisher = context.socket(zmq.PUB)
        publisher.bind(f"tcp://{self.addr}:{self.fe}")

        coinmarketcap = Market()
        cmc_ = coinmarketcap.listings()

        for i in cmc_['data']:
            if i['symbol'] not in self.cmc_list:
                self.cmc_list[i['symbol']] = {
                                                "name": i['name'],
                                                "id": i['id'],
                                             }
        while True:

            # -------------------------------------
            # process info 
            # -------------------------------------
            try:
                rcv = sub.recv(flags=zmq.NOBLOCK)
            except zmq.Again as e:
                pass
            else:

                topic, payload = rcv.split(b'@',1)               
                payload = json.loads(payload)
                exchange = payload['exchange']

                # ----------------------------------------------------------------------
                # Add coins from exchange_info to local dict self.marketcup['marketcup']
                # ----------------------------------------------------------------------

                if exchange not in self.marketcup:
                    self.marketcup[exchange] = {}
                
                for i in payload['active_symbols']:
                    target, market = i.split('/')
                    # -------------------------------
                    # marketcup by exchange
                    # -------------------------------
                    if target not in self.marketcup[exchange]:
                        self.marketcup[exchange][target] = {}
                    
                    if target not in self.marketcup['marketcup']:
                        self.marketcup['marketcup'][target] = {}
                    
                        if target in self.cmc_list: 
                            self.marketcup['marketcup'][target] = self.cmc_list[target]
                            
            

            # ---------------------------------------------------
            #  Request update from CoinMarketCum every x minutes
            # ---------------------------------------------------            
            if datetime.datetime.now() > self.next_cmc_update:
                if len(self.marketcup['marketcup']) > 0:
                    self.next_cmc_update = datetime.datetime.now() + datetime.timedelta(minutes=CMC_TICK_TIMER)
                
                for key, value in self.marketcup['marketcup'].items():
                    if 'id' in value.keys(): # Если такой коин есть на маркет кап
                        
                        try:
                            cmc_tick = coinmarketcap.ticker(value['id'], convert='USD')
                        except Exception as e:
                            print(self.name, e)
                        else:
                            if self.update_coin(key, cmc_tick['data']):
                                subscribe_str = f"cmc-{key}@"
                                publisher.send_string( subscribe_str + json.dumps(self.marketcup['marketcup'][key]) )
                                #print('updte for:',subscribe_str)
                    

                    sleep(1.5)
            # -------------------------------------
            #  Push collected CMC every X seconds 
            # -------------------------------------
            if datetime.datetime.now() > self.next_cmc_push_timer:
                self.next_cmc_push_timer = datetime.datetime.now() + datetime.timedelta(seconds=CMC_PUSH_TIMER)
                

                for i_coin in self.marketcup['marketcup'].keys():
                    if 'last_updated' in self.marketcup['marketcup'][i_coin].keys():
                        subscribe_str = f"cmc-{i_coin}@"
                        publisher.send_string( subscribe_str + json.dumps(self.marketcup['marketcup'][i_coin]) )
                        #print(self.name, i_coin, subscribe_str, self.marketcup['marketcup'][i_coin])

            sleep(0.1)