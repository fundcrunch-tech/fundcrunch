import queue, threading
from  multiprocessing import Process
from time import sleep
from datetime import datetime
import sys, json
import zmq
import asyncio

from drivers.exchdriver import ExchangeDriver
from drivers.cxxtdriver import CxxtDriver

class KucoinDriver(Process, ExchangeDriver):

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

        self.active_symbols = []
        
           
    def run(self):

        self.context = zmq.Context()
        self.sender = self.context.socket(zmq.PUSH)

        # if self.conf['zmq']['fe']['type'] == 'ipc':
        #     self.sender.connect('ipc://%s'%self.conf['zmq']['fe']['port'] )

        self.sender.connect('tcp://%s:%s' % (self.addr, self.port) )
        
        ccxt_loop = asyncio.new_event_loop()
        cx = threading.Thread(target=CxxtDriver, args=(ccxt_loop, self.cxxt_conf))
        cx.start()

        while True:

            #print(self.tnow(), self.name, 'tick')

            while not self.cxxt_conf['ccxt_out_queue'].empty():
                try:
                    ccxt_message = self.cxxt_conf['ccxt_out_queue'].get()
                except queue.Empty:
                    pass
                else:
                    #print(self.name, ccxt_message['symbol'], ccxt_message['message'])
                    if ccxt_message['message'] != 'error':
                        if ccxt_message['message'] == 'active_symbols':
                            self.active_symbols = ccxt_message['active_symbols']
                            self.sender.send_json( ccxt_message )
                        else:
                            self.sender.send_json( ccxt_message )                                                 
                
            sleep(0.05)

        cx.join()    