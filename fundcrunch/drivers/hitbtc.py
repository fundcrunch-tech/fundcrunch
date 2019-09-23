import queue, threading
from  multiprocessing import Process
from time import sleep
import datetime
import sys, json
import zmq
import asyncio
import requests
from operator import itemgetter
import itertools
import websockets

from hitbtc import HitBTC

from drivers.exchdriver import ExchangeDriver
from drivers.cxxtdriver import CxxtDriver

ORDER_BOOK_DETH = 20
INFO_TICK = 30

wss_url = 'wss://api.hitbtc.com/api/2/ws'

def list_chunk(it, size):
    it = iter(it)
    return iter(lambda: tuple(itertools.islice(it, size)), ())

class HitBtcWebSocketStreams(threading.Thread):
    def __init__(self, config, pairs_list):
        threading.Thread.__init__(self)
        self.config = config
        self.pname = '[hitbtc_ws]'

        self.is_auth = False
        if 'auth' in self.config.keys():
            self.is_auth = True
        
        self.local_symbols = dict()
        self.local_order_book = dict()

        self.pairs_list = pairs_list


        for i in self.pairs_list:
            self.local_symbols[ i.replace('/','') ] = i
            self.local_order_book[i.replace('/','')] = {'bids':[], 'asks':[]}

        print(self.pname, 'Initialized!')
    
    def build_ticker_mesage(self, data):
        return {'message': 'ticker',
                'exchange':'hitbtc',
                'symbol':data['symbol'],
                'i':data['data']['k']['i'],
                'o':data['data']['k']['o'],
                'h':data['data']['k']['h'],
                'l':data['data']['k']['l'],
                'c':data['data']['k']['c']}
        
    
    def build_trade_mesage(self, data):
        # {'data': [{'id': 309296935, 'price': '0.078857', 'quantity': '0.080', 'side': 'buy', 'timestamp': '2018-06-09T09:00:46.122Z'}]}
        t, symbol, payload = data
        ret = []
        #print(payload)
        for i in payload['data']:
            d = {'message': 'trade', 
                'exchange':'hitbtc',
                'symbol': self.local_symbols[symbol],
                'timestamp': i['timestamp'],
                'price': i['price'],
                'amount': i['quantity'],
                'side': i['side'],
                'id': i['id']
                }
            
            ret.append(d)
        return ret

    def init_local_order_book(self, data):
        t, symbol, payload = data

        bids = []
        asks = []
        
        for i in payload['ask']: 
            if float(i['size']) > 0:
                self.local_order_book[symbol]['asks'].append( {'price': float(i['price']), 'size':float(i['size']) } )
                asks.append([ float(i['price']), float(i['size']) ])
        
        for i in payload['bid']: 
            if float(i['size']) > 0:
                self.local_order_book[symbol]['bids'].append( {'price': float(i['price']), 'size':float(i['size']) }  )
                bids.append([ float(i['price']), float(i['size']) ])

        
        # if symbol == 'BCNBTC':
        #     print(asks[0])

        #     print('--%s--'%symbol)

        #     print(bids[0])
            
        #     print('')


        return {'message':'ob', 
                'exchange':'hitbtc', 
                'symbol': self.local_symbols[symbol], 
                'order_book': {'bids':bids[:ORDER_BOOK_DETH], 'asks':asks[:ORDER_BOOK_DETH] }  }


    def build_order_book_message(self, data):
        t, symbol, payload = data
        
        for i in payload['ask']: 
                
                new_order = True

                for j in self.local_order_book[symbol]['asks']:
                    if float(i['price']) == j['price']:

                        if float(i['size']) == 0:
                            #print(self.pname, t, symbol, 'ask', i['price'], i['size'], 'remove(%s)'%len(self.local_order_book[symbol]['asks']))
                            self.local_order_book[symbol]['asks'].remove(j)
                            new_order = False

                        elif float(i['size']) != float(j['size']):
                            #print(self.pname, t, symbol, 'update ask', j['price'], i['size'], 'update to', j['size'], 'len(%s)'%len(self.local_order_book[symbol]['asks']) )
                            j['size'] = float(i['size'])
                            new_order = False

                        break
                
                if new_order:
                    #print( self.pname, t, symbol, 'new ask', i['price'], i['size'], 'len(%s)'%len(self.local_order_book[symbol]['asks']) )
                    self.local_order_book[symbol]['asks'].append( {'price': float(i['price']), 'size':float(i['size']) } )
        

        for i in payload['bid']: 
                
                new_order = True

                for j in self.local_order_book[symbol]['bids']:
                    
                    if float(i['price']) == j['price']:

                        if float(i['size']) == 0:
                            self.local_order_book[symbol]['bids'].remove(j)
                            new_order = False

                        elif float(i['size']) != float(j['size']):
                            j['size'] = float(i['size'])
                            new_order = False

                        break
                
                if new_order:
                    self.local_order_book[symbol]['bids'].append( {'price': float(i['price']), 'size':float(i['size']) } )
                


        bids = [ [float(i['price']), float(i['size'])] for i in self.local_order_book[symbol]['bids'] ]
        asks = [ [float(i['price']), float(i['size'])] for i in self.local_order_book[symbol]['asks'] ]

        asks_ = sorted(asks, key=itemgetter(0))
        bids_ = sorted(bids, key=itemgetter(0), reverse=True)

        # if symbol == 'BCNBTC':
        #     for i in asks_[:ORDER_BOOK_DETH]:
        #         print(i)

        #     print('--%s--'%symbol)

        #     for i in bids_[:ORDER_BOOK_DETH]:
        #         print(i)
            
        #     print('')

  
        return {'message':'ob', 
                'exchange':'hitbtc', 
                'symbol': self.local_symbols[symbol], 
                'order_book': {'bids':bids[:ORDER_BOOK_DETH], 'asks':asks[:ORDER_BOOK_DETH] }  }
    
    def run(self):
        
        while True:

            try:
                wss = HitBTC(silent=True)
                wss.start()
                sleep(2)
                
            except Exception as e:
                print(self.pname, e, 'restart wss..')
                wss.close()
                del wss
                sleep(2)
            else:

                for i in self.local_symbols:
                    if 'trades' in self.config['mode']:
                        wss.subscribe_trades(symbol=i)
                    
                    sleep(0.5)
                    
                    if 'order_book' in self.config['mode']:
                        wss.subscribe_book(symbol=i)
                    
                
                
                while True:
                    try:
                        data = wss.recv()

                    except Exception as e:
                        print(self.pname, 'read()', e)
            
                    else:
                        t, symbol, payload = data
                        if t == 'updateTrades':
                            messages = self.build_trade_mesage(data)
                            for i in messages: self.config['web_socket_streams_out'].put( i )
                        elif t == 'updateOrderbook':
                            messages = self.build_order_book_message(data)
                            self.config['web_socket_streams_out'].put( message )
                            #print(self.pname, 'updateOrderbook', symbol)

                        elif t == 'snapshotOrderbook':
                            message = self.init_local_order_book(data)
                            self.config['web_socket_streams_out'].put( message )
                            print(self.pname, 'snapshotOrderbook')



class HitbtcDriver(Process, ExchangeDriver):

    def __init__(self, conf, addr, port):
        Process.__init__(self)
        ExchangeDriver.__init__(self, exchange=conf['exchange'], conf=conf)
        self.name = '[' + conf['exchange'] + ']'
        self.exchange_name = conf['exchange']

        self.info_tick = datetime.datetime.now()

        self.addr = addr
        self.port = port

        self.session = requests.Session()

        self.cxxt_conf = conf
        self.cxxt_conf['ccxt_in_queue'] = queue.Queue()
        self.cxxt_conf['ccxt_out_queue'] = queue.Queue()
        self.cxxt_conf['web_socket_streams_in'] = queue.Queue()
        self.cxxt_conf['web_socket_streams_out'] = queue.Queue()

        self.active_symbols = []

        self.is_auth = False
        if 'auth' in self.cxxt_conf.keys():
            self.is_auth = True

        # {'exchange': 'hitbtc',
        #                   'pairs': conf['pairs'],
        #                   'ccxt_in_queue': queue.Queue(),
        #                   'ccxt_out_queue': queue.Queue() }


    def featch_active_symbols(self):
        try:    
            r = self.session.get('https://api.hitbtc.com/api/2/public/symbol')

        except (requests.RequestException, requests.ConnectionError,
                requests.HTTPError, requests.URLRequired,
                requests.TooManyRedirects, requests.ConnectTimeout,
                requests.ReadTimeout, requests.Timeout) as e:
                
                print(self.name, 'featch_active_symbols', e)
        
        else:
            if r.status_code == requests.codes.ok:
                hotbtc_symbols = json.loads(r.text)
                self.active_symbols = [ '%s/%s'%(i['baseCurrency'], i['quoteCurrency']) for i in  hotbtc_symbols]
                info = {'message': 'active_symbols', 
                        'exchange':'hitbtc', 
                        'active_symbols':self.active_symbols}
                self.sender.send_json( info )

    def run(self):

        self.context = zmq.Context()
        self.sender = self.context.socket(zmq.PUSH)

        cx = None
        wsp = None

        # if self.conf['zmq']['fe']['type'] == 'ipc':
        #     self.sender.connect('ipc://%s'%self.conf['zmq']['fe']['port'] )

        self.sender.connect('tcp://%s:%s' % (self.addr, self.port) )

        if self.is_auth:
            ccxt_loop = asyncio.new_event_loop()
            self.cxxt_conf['mode'] = ['balance']
            #self.cxxt_conf['pairs'].append('BTC/USDT')
            cx = threading.Thread(target=CxxtDriver, args=(ccxt_loop, self.cxxt_conf))
            cx.start()
        
        else:
            
            self.featch_active_symbols()

            if self.cxxt_conf['pairs']:
                self.active_symbols = self.cxxt_conf['pairs']
                print(self.name, 'preconfigured symbols..', self.active_symbols)
            else:
                print(self.name, 'featching for all pairs..', len(self.active_symbols))

            # ccxt_loop = asyncio.new_event_loop()
            # self.cxxt_conf['mode'] = []
            # self.cxxt_conf['pairs'] = self.active_symbols
            # cx = threading.Thread(target=CxxtDriver, args=(ccxt_loop, self.cxxt_conf))
            # cx.start()

            
            self.cxxt_conf['mode'] = ['order_book','trades', 'ohlc']
            self.cxxt_conf['pairs'] = self.active_symbols
            wsp = HitBtcWebSocketStreams ( self.cxxt_conf, self.active_symbols )
            wsp.start()


        while True:

            #  ======================
            #  Очередь из CCXT
            #  ======================

            while not self.cxxt_conf['ccxt_out_queue'].empty():
                try:
                    ccxt_message = self.cxxt_conf['ccxt_out_queue'].get_nowait()
                except queue.Empty:
                    pass
                else:
                    if ccxt_message['message'] != 'error':
                        if ccxt_message['message'] == 'active_symbols':
                            # self.active_symbols = ccxt_message['active_symbols']
                            # self.sender.send_json( ccxt_message )
                            pass
                        else:                            
                            self.sender.send_json( ccxt_message )
            
            #  ======================
            #  Очередь из WebSoket
            #  ======================
            if not self.is_auth:
                while not self.cxxt_conf['web_socket_streams_out'].empty():
                    try:
                        ws_message = self.cxxt_conf['web_socket_streams_out'].get_nowait()
                    except queue.Empty:
                        pass
                    else:
                        #print(self.name, ws_message['message'], ws_message['symbol'])
                        self.sender.send_json( ws_message )
                        pass

            # =================================
            # Info Tick
            # =================================
            if datetime.datetime.now() > self.info_tick:
                self.info_tick = datetime.datetime.now() + datetime.timedelta(seconds=INFO_TICK)
                
                self.featch_active_symbols()
                    

            sleep(0.05)

        if cx: cx.join()
        if wsp: wsp.join()
        if ws_chanjs_th: [i.join() for i in ws_chanjs_th]
