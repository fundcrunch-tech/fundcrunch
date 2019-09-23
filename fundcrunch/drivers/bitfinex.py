import queue, threading
from  multiprocessing import Process
from time import sleep
from datetime import datetime
import sys, json
import zmq
import asyncio

from drivers.exchdriver import ExchangeDriver
from drivers.cxxtdriver import CxxtDriver

pairs = ['FUN/ETH']

class BitfinexDriver(Process, ExchangeDriver):

    def __init__(self, conf, addr, port):
        Process.__init__(self)
        ExchangeDriver.__init__(self, exchange=conf['exchange'], conf=conf)
        self.name = '[' + conf['exchange'] + ']'
        self.exchange_name = conf['exchange']

        self.addr = addr
        self.port = port

        self.cxxt_conf = conf
        self.cxxt_conf['ccxt_in_queue'] = queue.Queue()
        self.cxxt_conf['ccxt_out_queue'] = queue.Queue()
        self.cxxt_conf['web_socket_streams_in'] = queue.Queue()
        self.cxxt_conf['web_socket_streams_out'] = queue.Queue()

        self.active_symbols = []

    def run(self):

        self.context = zmq.Context()
        self.sender = self.context.socket(zmq.PUSH)

        self.sender.connect('tcp://%s:%s' % (self.addr, self.port) )

        ccxt_loop = asyncio.new_event_loop()
        cx = threading.Thread(target=CxxtDriver, args=(ccxt_loop, self.cxxt_conf))
        cx.start()

        while True:

            #  ======================
            #  Очередь из CCXT
            #  ======================
            while not self.cxxt_conf['ccxt_out_queue'].empty():
                try:
                    ccxt_message = self.cxxt_conf['ccxt_out_queue'].get()
                except queue.Empty:
                    pass
                else:
                    if ccxt_message['message'] != 'error':
                        if ccxt_message['message'] == 'active_symbols':
                            self.active_symbols = ccxt_message['active_symbols']
                            self.sender.send_json( ccxt_message )
                        else:
                            #print(ccxt_message)
                            self.sender.send_json( ccxt_message )

            sleep(0.05)

        cx.join()
