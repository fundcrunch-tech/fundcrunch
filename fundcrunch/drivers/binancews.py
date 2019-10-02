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
import urllib.request
import operator
import math, random

from .exchdriver import ExchangeDriver
from .ccxtdriver import CcxtDriver

wss = 'wss://stream.binance.com:9443'
depth_url = 'https://www.binance.com/api/v1/depth?' #+symbol=BNBBTC&limit=1000
obdepth = 20

class SnapshotAgent(threading.Thread):
    def __init__(self, symbol, queue):
        threading.Thread.__init__(self)
        self.symbol= symbol
        self.queue = queue

    def run(self):
        time.sleep(2)
        s = self.symbol.replace('/','')
        try:
            contents = urllib.request.urlopen(depth_url+'symbol=%s&limit=%s'%(s,obdepth)).read()
        except Exception as e:
            print(self.name, '[ERROR]', depth_url+'symbol=%s&limit=%s'%(s,obdepth), e)
        else:
            contents = json.loads(contents)

            contents['symbol']=self.symbol
            self.queue.put(contents)
            #print(self.symbol, 'ob snapshot downloaded')
        


class BinanceWebSocketStreams(threading.Thread):
    def __init__(self, loop, config):

        self.config = config
        self.pname = '[binance_ws]'

        self.ob_update_queue = []
        self.ob_snapshot_queue = queue.Queue()
        self.orderbook_local={}

        self.is_auth = False

        self.ob_bids_original = []
        self.ob_asks_original = []
        self.ob_lastUpdateId = None

        self.ob_bids = []
        self.ob_asks = []

        self.trade_bid = None
        self.trade_ask = None
        

        if 'auth' in self.config.keys():
            self.is_auth = True

        print(self.pname, 'started with %s pairs'%len(self.config['pairs']), self.config['mode'] )

        for i in self.config['pairs']:
            self.orderbook_local[i] = {'updates':[], 'orderbook':{} }
            t = SnapshotAgent(i,self.ob_snapshot_queue)
            t.start()

        client1 = loop.run_until_complete(self.web_socket_streams( self.config['pairs'] ) )

        
        self.active_symbols = []


        try:
            loop.run_forever()
        except KeyboardInterrupt:
            pass

        client1.close()
        

    def build_ohlc_mesage(self, data):
        res = None
        
        if data['data']['k']['x']:
            
            # -------------------------------------
            # aligne symbnol name to normal
            # --------------------------------------

            symbol = data['data']['s']
            if symbol[-3:] in ['ETH','BTC','BNB','PAX']:
                taget_len = len(symbol)-3
                symbol = symbol[:taget_len] +'/'+ symbol[taget_len:]

            elif symbol[-4:] in ['USDT', 'TUSD', 'USDC', 'USDS']:
                taget_len = len(symbol)-4
                symbol = symbol[:taget_len] +'/'+ symbol[taget_len:]
            
            res = {'message': 'ohlc',
                   'exchange':'binance',
                'symbol':symbol,
                'i':data['data']['k']['i'],
                'o':data['data']['k']['o'],
                'h':data['data']['k']['h'],
                'l':data['data']['k']['l'],
                'c':data['data']['k']['c'],
                't':data['data']['k']['t'],
                'T':data['data']['k']['T'],
                'v':data['data']['k']['v'],
                'info': data['data']['k'],
                }
        
        return res

    def build_trade_mesage(self, data):
        # 'data': {'e': 'trade', 'E': 1528205859842, 's': 'BTCUSDT', 't': 48621336, 'p': '7422.04000000', 'q': '0.95822400', 'b': 113778307, 'a': 113778288, 'T': 1528205859838, 'm': False, 'M': True}, 'symbol': 'BTC/USDT'}
        # if data['symbol'] == 'ETH/BTC':
        #     print(data['symbol'], data['data']['e'], data['data']['q'], data['data']['p'],
        #             #time.strftime('%H:%M:%S', time.gmtime(data['data']['T'])), 
        #             datetime.datetime.now() - datetime.datetime.fromtimestamp(int(data['data']['T'])/1000),
        #             datetime.datetime.now().strftime('%H:%M:%S') )
                    
        #if data['symbol'] == 'ETH/BTC':
            # print(data['symbol'], 
            #         datetime.datetime.now() - datetime.datetime.fromtimestamp(int(data['data']['T'])/1000),
            #         datetime.datetime.now().strftime('%H:%M:%S'), data['data']['p'], data['data']['q'], data['data']['m'])
        
        side = None
            #market ask True
        if data['data']['m']:
            side = 'buy'
        else:
            side = 'sell'     
        

        return {'message': 'trade',
                'exchange':'binance',
                'symbol':data['symbol'],
                'timestamp': data['data']['T'],
                'price': data['data']['p'],
                'amount': data['data']['q'],
                'side': side,
                'info': data['data']
                }
    
    def order_book_snapshot(self,data):
        # -----------------------
        # Init Local OrderBook
        # -----------------------
        #self.orderbook_local[i] = {'updates':[], 'orderbook':{} }
        d = {'bids':[], 'asks':[], 'lastUpdateId':None}
        s = data['symbol'].replace('/','')
        #contents = urllib.request.urlopen(depth_url+'symbol=%s&limit=%s'%(s,obdepth)).read()
        #contents = json.loads(contents)
        d['bids'] = [ [float(item[0]),float(item[1])] for item in data['bids'] ]
        d['asks'] = [ [float(item[0]),float(item[1])] for item in data['asks'] ]
        d['lastUpdateId'] = data['lastUpdateId']
        self.orderbook_local[data['symbol']]['orderbook']=d
        #print(data['symbol'], 'orderbook_local initiated !!!')  

    def ob_merge_update(self, symbol, update, ob):

        # if symbol == 'ETH/BTC':
        #     print( [i for i in update['b']] )
        #     print( [i for i in update['a']] )
        # print(ob['bids']) 
        # print(ob['bids'])

        # print(update['b']) 
        # print(update['a'])

        for i in update['b']:
            new_level=True      
            for j in ob['bids']:
                #print(type(float(i[0])), float(i[0]),  type(j[0]), j[0])
                if math.isclose( float(i[0]), j[0], rel_tol=1e-09, abs_tol=0.0):#i[0] == j[0]: #same level matched
                    new_level=False    
                    if math.isclose(float(i[1]), 0.0, rel_tol=1e-9):
                        ob['bids'].remove(j)
                    else:                        
                        j[1] = float(i[1])
                        #print(symbol, 'update:', j, len(ob['bids']))     

            if new_level and not math.isclose(float(i[1]), 0.0, rel_tol=1e-9):
                ob['bids'].append( [float(i[0]), float(i[1])] )
                #print(symbol, 'new_level:', j, len(ob['bids']))
        
        for i in update['a']:
            new_level=True            
            for j in ob['asks']:
                if math.isclose(float(i[0]), float(j[0]), rel_tol=1e-09, abs_tol=0.0): #i[0] == j[0]: #same level matched
                    new_level=False
                    if math.isclose(float(i[1]), 0.0, rel_tol=1e-1):
                        ob['asks'].remove(j)
                    else:                        
                        j[1] = float(i[1])                      
            if new_level and not math.isclose(float(i[1]), 0.0, rel_tol=1e-9):
                ob['asks'].append([float(i[0]), float(i[1])])
            
       
        ob['asks'] = sorted(ob['asks'], key=operator.itemgetter(0))[:obdepth]
        ob['bids'] = sorted(ob['bids'], key=operator.itemgetter(0), reverse=True )[:obdepth]
        

        # if symbol == 'ETH/BTC':
        #     print(symbol, ob['asks'][0][0], ob['bids'][0][0])
        
        try:
            if len(ob['asks']) > 1:
                if float(ob['asks'][0][0]) > float(ob['asks'][1][0]):
                    print(self.pname, '[AHTUNG]')
                    print(ob)
                    exit()
                    
        except:
            print(self.pname, '[ERROR]')
            print(ob)
            exit()



    def update_local_orderbook(self,data):
        to_send = []
        self.orderbook_local[data['symbol']]['updates'].append(data)
        
        symbol = data['symbol']
        ob = self.orderbook_local[symbol]['orderbook']
        up = self.orderbook_local[symbol]['updates']

        if ob:
            
            for u in up:
                if u['data']['u'] <= ob['lastUpdateId']:
                    up.remove(u)
                    #print(data['symbol'], 'remove', u['data']['u'], ob['lastUpdateId'])

                elif u['data']['U'] <= (ob['lastUpdateId']+1) and u['data']['u']>=(ob['lastUpdateId']+1):

                    # if data['symbol'] == 'ETH/BTC':
                    #     print(symbol, data['data']['U'], ob['lastUpdateId']+1, data['data']['u'])

                    self.ob_merge_update(data['symbol'],u['data'], ob)
                    ob['lastUpdateId']=u['data']['u']
                    up.remove(u)

                    to_send = {'message':'ob',
                               'exchange':'binance',
                               'symbol':data['symbol'],
                               'order_book': {'bids':ob['bids'], 'asks':ob['asks'], 'timestamp': time.time() }  }


                else:
                    #print(data['symbol'], 'remove', u['data']['U'],  ob['lastUpdateId']+1, u['data']['u'])
                    up.remove(u)
                    t = SnapshotAgent(data['symbol'], self.ob_snapshot_queue)
                    t.start()
                
                # if data['symbol'] == 'ETH/BTC':
                #     print(data['symbol'], 
                #           datetime.datetime.now() - datetime.datetime.fromtimestamp(int(data['data']['E'])/1000),
                #           datetime.datetime.now().strftime('%H:%M:%S'))
        else:
            
            # print(data['symbol'], 'no ob snapshot', 
            #         datetime.datetime.now() - datetime.datetime.fromtimestamp(int(data['data']['E'])/1000),
            #         datetime.datetime.now().strftime('%H:%M:%S'))
            pass
        
        return to_send

 
    async def ws_connect(self, uri):
        ws = None
        
        try:
            #uri='wss://stream.binance.com:9443/stream?streams=adabnb@depth/adabnb@trade/adabnb@kline_1m/'
            #print(uri)
            ws = await websockets.connect(uri = uri, ssl=True) # '/ws/bnbbtc@depth20'
        except websockets.exceptions.ConnectionClosed as e:
            print( e )
            #print(uri)
            ws = None
        
        return ws


    async def web_socket_streams(self, symbols):

        '''
        {'name':'BTC/USD',
         'stream':btcusd@depth20 }
        '''

        #for i in (self.config['pairs']+['BTC/USDT']):
        active_symbols_url = []
        subscribed_symbols = []
   


        for i in self.config['pairs']:
            t,b = i.split('/')
            prefix = t.lower() + b.lower()
            subscribed_symbols.append(prefix)
            active_symbols_url.append({'name':i,
                                       'depth':prefix+'@depth',
                                       #'depthx':prefix+'@depth%s'%obdepth,
                                       'trade':prefix+'@trade',
                                       'candle':[prefix+'@kline_1m', 
                                                 prefix+'@kline_3m',
                                                 prefix+'@kline_5m',
                                                 prefix+'@kline_15m',
                                                 prefix+'@kline_30m',
                                                 prefix+'@kline_1h',
                                                 prefix+'@kline_2h',
                                                 prefix+'@kline_4h',
                                                 prefix+'@kline_6h',
                                                 prefix+'@kline_8h',
                                                 prefix+'@kline_12h',
                                                 prefix+'@kline_1d',
                                                 prefix+'@kline_3d',
                                                 prefix+'@kline_1w',
                                                 prefix+'@kline_1M'
                                                 ]
                                       })

        #while True:
            
        streams = '/stream?streams='
        
        for i in active_symbols_url:
            candles = str()
            for j in i['candle']:
                candles += j + '/'
            streams += i['depth']+'/'+i['trade']+'/'+candles #i['candle']+'/'
        

        streams = streams[:-1]
        
        # for 24 hours disconnect
        while True:

           
                #ws = await websockets.connect(uri = wss + streams, ssl=True) as ws: # '/ws/bnbbtc@depth20'
                
            while True:
                try:
                    ws = await self.ws_connect(wss + streams)                
                    
                except Exception as e:
                    print( e, subscribed_symbols)
                    self.config['web_socket_streams_out'].put( {'message':'error',
                                                                'payload': "ws_coonect() exception"  } )
                else:
                    if ws:
                        self.config['web_socket_streams_out'].put( {'message':'info',
                                                                    'payload': f"ws_coonect() {subscribed_symbols} ok"  } )
                        break
        
                    else:
                        self.config['web_socket_streams_out'].put( {'message':'info',
                                                                    'payload': f"ws_coonect() {subscribed_symbols} 400"  } )
                        sleep(360)

            
            while True:
                try:
                   
                    m = await ws.recv()
                    
                except Exception as e:
                    print(e, subscribed_symbols)
                    ws.close()
                    self.config['web_socket_streams_out'].put( {'message':'error',
                                                                'payload': "ws.recv() exception"  } )

                    break
                else:

                    jd = json.loads(m)

                    

                    to_send = None
                    for sm in active_symbols_url:
                        if jd['stream'] in sm.values():
                            jd['symbol'] = sm['name']
                            break

                    # if '@depth%s'%obdepth in jd['stream']:
                    #     to_send = self.build_order_book_message(jd)
                    if '@depth' in jd['stream']:
                        to_send = self.update_local_orderbook(jd)
                    elif '@kline' in jd['stream']:
                        to_send = self.build_ohlc_mesage(jd)
                    elif '@trade' in jd['stream']:
                        to_send = self.build_trade_mesage(jd)
                    else:
                        print(jd)

                    if to_send:
                        self.config['web_socket_streams_out'].put( to_send )


                    try:
                        data = self.ob_snapshot_queue.get_nowait()
                    except queue.Empty:
                        pass
                    else:
                        self.order_book_snapshot(data)
                
            
            
        


            
