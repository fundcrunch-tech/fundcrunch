import queue, threading
from  multiprocessing import Process
from time import sleep
import datetime
import sys, json
import zmq
import asyncio
import requests

from drivers.exchdriver import ExchangeDriver
from drivers.cxxtdriver import CxxtDriver

from .latokenws import LAWS

ENDPOINT_URL = 'https://api.latoken.com/'
#ENDPOINT_URL = 'http://46.101.130.112:27384/'
TIMEOUT = 5
BALANCE_TIMER = 30
ORDERBOOK_LOOP_TIMER = 3


class LatokenDriver(Process, ExchangeDriver):

    def __init__(self, conf, addr, port):

        Process.__init__(self)
        ExchangeDriver.__init__(self, exchange=conf['exchange'], conf=conf)
        self.name = '[' + conf['exchange'] + ']'
        self.exchange_name = conf['exchange']

        self.cookie = conf['auth']['apiKey']

        self.addr = addr
        self.port = port

        self.run_locker = True

        # {'exchange':'binance', 'pairs':['FUN/ETH'],
        # 'auth':LA_binance, 'mode':['balance'],
        # 'zmq':{ 'fe' : { 'type':'ipc', 'port':5558} }},

        self.config = conf
        self.config['ccxt_in_queue'] = queue.Queue()
        self.config['ccxt_out_queue'] = queue.Queue()

        self.currency_id = dict()
        self.latocken_pairs_id = dict()

        self.ws_out_queue = queue.Queue()

        new_loop = asyncio.new_event_loop()
        self.wsp = threading.Thread(target=LAWS, args=(new_loop, self.ws_out_queue, self.cookie))
        self.wsp.start()

    def __del__(self):
        self.wsp.join()


    def get_exchange_info(self):
        try:
            r = self.session.post(ENDPOINT_URL + 'initiate/', timeout=20)

        except (requests.RequestException, requests.ConnectionError,
                requests.HTTPError, requests.URLRequired,
                requests.TooManyRedirects, requests.ConnectTimeout,
                requests.ReadTimeout, requests.Timeout) as e:

            pass

        else:
            if r.status_code == requests.codes.ok:
                latoken_data = json.loads(r.text)

                for i  in latoken_data['currencies']:
                    self.currency_id [i['shortName']] = i['id']

        try:
            r = self.session.get(ENDPOINT_URL + 'v1/trading/tradingPairs', timeout=TIMEOUT)
        except (requests.RequestException, requests.ConnectionError,
                requests.HTTPError, requests.URLRequired,
                requests.TooManyRedirects, requests.ConnectTimeout,
                requests.ReadTimeout, requests.Timeout) as e:

            print(self.name, 'ERROR', 'v1/trading/tradingPairs', e)
            self.run_locker = False

        else:
            if r.status_code == requests.codes.ok:
                latoken_data = json.loads(r.text)

                for i in latoken_data:
                    self.latocken_pairs_id[ i['symbol'] ] = i['id']


    def run(self):

        self.session = requests.Session()
        self.session.headers.update({'x-zalogo-id': 'zsi1ff5b7c9855fc0af9409287368df5c55'})

        self.context = zmq.Context()
        self.sender = self.context.socket(zmq.PUSH)

        self.sender.connect('tcp://%s:%s' % (self.addr, self.port) )

        self.get_exchange_info()

        print(self.name, 'startting %s pairs'% len(self.config['pairs']))

        balance_timer = datetime.datetime.now()
        orderbook_timer = datetime.datetime.now() + datetime.timedelta(seconds= ORDERBOOK_LOOP_TIMER )

        while True:
            sleep(0.05)
            #==========================
            # balance
            #==========================
            if datetime.datetime.now() > balance_timer:
                balance_timer = datetime.datetime.now() + datetime.timedelta(seconds=BALANCE_TIMER)

                for i in self.config['pairs']:

                    try:
                        r = self.session.get(ENDPOINT_URL + 'assets/getBaseAssetParams/',
                                                                params={'tradingPairId': self.latocken_pairs_id[i] },
                                                                timeout=5)

                    except (requests.RequestException, requests.ConnectionError,
                            requests.HTTPError, requests.URLRequired,
                            requests.TooManyRedirects, requests.ConnectTimeout,
                            requests.ReadTimeout, requests.Timeout) as e:

                            print(self.name, 'get_asset', e)

                    else:
                        if r.status_code == requests.codes.ok:
                            latoken_data = json.loads(r.text)

                            for bl in latoken_data['data']['balances']:
                                curr_name = None

                                for k,v in self.currency_id.items():
                                    #print(k,v, bl['currencyId'])
                                    if bl['currencyId'] == v:
                                        #print(self.name,k,v)
                                        curr_name = k
                                    if curr_name:
                                        break

                                if curr_name:
                                    #print(self.name, 'balance', curr_name)
                                    balance = {'message': 'bl',
                                            'account': self.config['auth']['apiKey'][:4]+'*',
                                            'exchange': 'latoken',
                                            'symbol': curr_name,
                                            'balance': {'free': bl['amount'], 'used': bl['deposited'], 'total': bl['amount']+bl['deposited']}}

                                    self.sender.send_json( balance )


            #=======================
            # order_book
            #=======================
            if datetime.datetime.now() > orderbook_timer:
                orderbook_timer = datetime.datetime.now() + datetime.timedelta(seconds=ORDERBOOK_LOOP_TIMER)

                for i in self.config['pairs']:
                    try:
                        r = self.session.get(ENDPOINT_URL + 'assets/getBaseAssetParams/',
                                                                params={'tradingPairId': self.latocken_pairs_id[i] },
                                                                timeout=5)
                    except (requests.RequestException, requests.ConnectionError,
                            requests.HTTPError, requests.URLRequired,
                            requests.TooManyRedirects, requests.ConnectTimeout,
                            requests.ReadTimeout, requests.Timeout) as e:

                        print(self.name, 'get_asset', e)

                    else:
                        if r.status_code == requests.codes.ok:
                            latoken_data = json.loads(r.text)

                            bids = []
                            asks = []

                            for s in latoken_data['data']['sellOrders'][::-1]:
                                asks.append([ s['price'], s['amount'] ])

                            for s in latoken_data['data']['buyOrders']:
                                bids.append([ s['price'], s['amount']])

                            order_book = {'message': 'ob',
                                        'exchange': 'latoken',
                                        'symbol': i,
                                        'order_book': {'bids':bids,
                                                        'asks':asks}
                                        }

                            self.sender.send_json( order_book )



                        #self.sender.send_json( ccxt_message )
