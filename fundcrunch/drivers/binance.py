import queue, threading
from  multiprocessing import Process
from time import sleep
import datetime, time
import sys, json, logging
import zmq
import asyncio, requests
import websockets
import itertools
import urllib.request


from .exchdriver import ExchangeDriver
from .ccxtdriver import CcxtDriver
from .binancews import BinanceWebSocketStreams
from .binancews_private import BinanceWebSocketPrivateStreams


logger = logging.getLogger()
logging.getLogger().setLevel(logging.INFO)
c_handler = logging.StreamHandler()
f_handler = logging.FileHandler('binance.log', mode='a')
c_handler.setLevel(logging.INFO)
f_handler.setLevel(logging.INFO)
c_format = logging.Formatter('%(asctime)s %(message)s', datefmt='%H:%M:%S')
f_format = logging.Formatter('%(asctime)s %(message)s', datefmt='%H:%M:%S')
c_handler.setFormatter(c_format)
f_handler.setFormatter(f_format)
logger.addHandler(c_handler)
logger.addHandler(f_handler)


INFO_TICK = 30
WSDRIVER_CHANCK = 25

def list_chunk(it, size):
    it = iter(it)
    return iter(lambda: tuple(itertools.islice(it, size)), ())

class BinanceDriver(Process, ExchangeDriver):

    def __init__(self, conf, addr, port):
        Process.__init__(self)
        ExchangeDriver.__init__(self, exchange=conf['exchange'], conf=conf)
        self.name = '[' + conf['exchange'] + ']'
        self.exchange_name = conf['exchange']

        self.info_tick = datetime.datetime.now()

        self.addr = addr
        self.port = port

        self.cxxt_conf = conf
        self.cxxt_conf['ccxt_in_queue'] = queue.Queue()
        self.cxxt_conf['ccxt_out_queue'] = queue.Queue()
        self.cxxt_conf['web_socket_streams_in'] = queue.Queue()
        self.cxxt_conf['web_socket_streams_out'] = queue.Queue()
        self.cxxt_conf['logger'] = logger
        self.general_queue = queue.Queue()
        self.dq_queue = queue.Queue()

        self.is_auth = False

        if 'auth' in self.cxxt_conf.keys():
            self.is_auth = True

        self.chTh = []

    def __del__(self):
        for i in self.chTh:
            i.join()

    def ws_queue_th(self):
        while True:
            #if not self.is_auth:
                #while not self.cxxt_conf['web_socket_streams_out'].empty():
            try:
                ws_message = self.cxxt_conf['web_socket_streams_out'].get()
            except queue.Empty:
                pass
            else:
                self.general_queue.put({'source': 'ws','msg': ws_message})
    
    def ccxt_queue_th(self):
        try:
            ccxt_message = self.cxxt_conf['ccxt_out_queue'].get_nowait()
        except queue.Empty:
            pass
        else:
            if ccxt_message['message'] != 'error':
                if ccxt_message['message'] == 'active_symbols':
                    self.cxxt_conf['web_socket_streams_in'].put(ccxt_message)
                    info_msg = ccxt_message
                    self.general_queue.put({'source':'ccxt', 'msg': ccxt_message})
                else:
                    #print(self.name, self.is_auth, ccxt_message['message'])
                    if self.is_auth:
                        ccxt_message['auth'] = self.cxxt_conf['auth']['apiKey']
                    self.general_queue.put({'source':'ccxt', 'msg': ccxt_message})

    def run(self):
        try:
            is_first_loop = True
            self.context = zmq.Context()
            self.sender = self.context.socket(zmq.PUSH)

            cx = None
            wsp = None

            info_msg = None

            self.sender.connect('tcp://%s:%s' % (self.addr, self.port)   )

            print(self.name, 'PUSH', self.addr, self.port)


            if self.is_auth:

                ccxt_loop = asyncio.new_event_loop()
                self.cxxt_conf['mode'] = []
                cx = threading.Thread(target=CcxtDriver, args=(ccxt_loop, self.cxxt_conf))
                cx.start()
                self.chTh.append(cx)


                conf = self.cxxt_conf.copy()
                wsp_loop = asyncio.new_event_loop()
                wsp = threading.Thread(target=BinanceWebSocketPrivateStreams, args=(conf, wsp_loop))
                self.chTh.append(wsp)
                wsp.start()
            
            else:

                ccxt_loop = asyncio.new_event_loop()
                self.cxxt_conf['mode'] = ['info']
                cx = threading.Thread(target=CcxtDriver, args=(ccxt_loop, self.cxxt_conf))
                cx.start()
                self.chTh.append(cx)

                if self.cxxt_conf['pairs']:
                    self.active_symbols = self.cxxt_conf['pairs']
                    print(self.name, 'preconfigured symbols..', self.active_symbols)
                else:
                    # ===============================
                    # Список всех доступных символов
                    while True:
                        message = self.cxxt_conf['ccxt_out_queue'].get()
                        if message['message'] == 'active_symbols':
                            self.active_symbols = message['active_symbols']
                            break

                    print(self.name, 'featching all symbols:', len(self.active_symbols))

                # =========================================
                # Деление списка символов на чанки
                chank = list_chunk( self.active_symbols, WSDRIVER_CHANCK )

                # =========================================
                #  thread для каждого чанка
                ws_chanjs_th = []
                for i in chank:
                    loop = asyncio.new_event_loop()
                    conf = self.cxxt_conf.copy()
                    conf['mode'] = ['order_book','trades', 'ohlc']
                    conf['pairs'] = list(i)
                    wsp = threading.Thread(target=BinanceWebSocketStreams, args=(loop, conf ))
                    # ws_chanjs_th.append(wsp)
                    self.chTh.append(wsp)
                    wsp.start()
                    sleep(5)
                

            wsth = threading.Thread(target=self.ws_queue_th)
            ccxtth =threading.Thread(target=self.ccxt_queue_th)

            wsth.start()
            ccxtth.start()

            self.chTh.append(wsth)
            self.chTh.append(ccxtth)
        
            while True:

                # #  ======================
                # #  Очередь из CCXT
                # #  ======================
                # #while not self.cxxt_conf['ccxt_out_queue'].empty():
                # try:
                #     ccxt_message = self.cxxt_conf['ccxt_out_queue'].get_nowait()
                # except queue.Empty:
                #     pass
                # else:
                #     if ccxt_message['message'] != 'error':
                #         if ccxt_message['message'] == 'active_symbols':
                #             self.cxxt_conf['web_socket_streams_in'].put(ccxt_message)
                #             info_msg = ccxt_message
                #             self.sender.send_json( ccxt_message )
                #         else:
                #             #print(self.name, self.is_auth, ccxt_message['message'])
                #             if self.is_auth:
                #                 ccxt_message['auth'] = self.cxxt_conf['auth']['apiKey']
                #             self.sender.send_json( ccxt_message )


                # #  ======================
                # #  Очередь из WebSoket
                # #  ======================
                # #if not self.is_auth:
                #     #while not self.cxxt_conf['web_socket_streams_out'].empty():
                # try:
                #     ws_message = self.cxxt_conf['web_socket_streams_out'].get_nowait()
                # except queue.Empty:
                #     pass
                # else:
                #     self.sender.send_json(ws_message)
                    

                # =================================
                # Info Tick
                # =================================
                if datetime.datetime.now() > self.info_tick:
                    self.info_tick = datetime.datetime.now() + datetime.timedelta(seconds=INFO_TICK)

                    if info_msg:
                        self.sender.send_json( info_msg )
                
                if is_first_loop:
                    is_first_loop = False
                    self.general_queue.queue.clear()


                message = self.general_queue.get()
                
                if message['msg']['message'] in ['error','info']:
                    logging.info(message['msg'])
                else:
                    self.sender.send_json(message['msg'])

                #==========================
                # Write all trades to DB
                #==========================
                # if message['source'] == 'ws':
                #     self.dq_queue.put(message['msg'])
        
        except KeyboardInterrupt as e:
            print(self.name,  'KeyboardInterrupt')
            sys.exit(0)



