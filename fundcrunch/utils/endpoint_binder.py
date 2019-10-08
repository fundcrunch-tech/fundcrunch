import zmq
import time
import sys, json
from random import randrange, randint
import random
import queue, threading, multiprocessing
import binascii
import gzip
import pprint

from .. import drivers
from .endpoint import SocketEndppoint, PushPub

class Feeder(threading.Thread):
    
    output = queue.Queue()

    def __init__(self, config, subscribe):
        threading.Thread.__init__(self)
        self.config = config
        self.subheaders = subscribe
        self.childs = []

        for i in config['exchanges']:
            driver_config = {"exchange": i['name'],
                    "pairs":i['pairs'],
                    "mode": i["mode"],
                    "output": multiprocessing.Queue()
                    }
            
            public_drv = drivers.BinanceDriver( conf=driver_config, addr=config['addr'], port=config['port'][0] )
            public_drv.start()
            self.childs.append(public_drv)
        
        pull_pub = PushPub("drivers_pull_pub",
                            pull_address= f"{config['addr']}:{config['port'][0]}",
                            pub_address= f"0.0.0.0:{config['port'][1]}")
        pull_pub.start()
        self.childs.append(pull_pub)

        self.endpoint = SocketEndppoint(sources=[ f"0.0.0.0:{config['port'][1]}" ], 
                                        endpoint=f"0.0.0.0:{config['port'][2]}")
        self.endpoint.start()
        self.childs.append(self.endpoint)
        
    
    def socket_listener(self):
        context = zmq.Context()
        subscriber = context.socket(zmq.SUB)
        subscriber.connect(f"tcp://{self.config['addr']}:{self.config['port'][2]}")
        for i in self.subheaders:
            subscriber.setsockopt(zmq.SUBSCRIBE, f"{i}@".encode())
           
        try:
            while True:
                try:
                    rcv = subscriber.recv()
                except zmq.Again as e:
                    print(e)
                else:
                    # topic, payload = rcv.split(b'@',1)
                    # payload = gzip.decompress(payload)
                    # payload = json.loads(payload)
                    self.output.put(rcv)
        except KeyboardInterrupt as e:
            pass
        
    def queue_listener(self, q):
        try:
            while True:
                try:
                    rcv = q.get()
                    self.output.put(rcv)
                except Exception as e:
                    print(e)
        except KeyboardInterrupt as e:
            pass

    def run(self):
        thread_id = []

        th = threading.Thread(target=self.socket_listener)
        th.start()
        thread_id.append(th)
    
        try:
            for i in thread_id:
                i.join()

            for i in self.childs:
                i.join()

        except KeyboardInterrupt as e:
            pass

        