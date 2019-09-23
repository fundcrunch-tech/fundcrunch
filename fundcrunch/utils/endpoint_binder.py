import zmq
import time
import sys, json
from random import randrange, randint
import random
import queue, threading
import binascii
import gzip
import pprint


class EndpointsBinder(threading.Thread):
    
    output = queue.Queue()

    def __init__(self, sockets, queues):
        threading.Thread.__init__(self)
        self.sockets = sockets
        self.qs = queue
        
    
    def socket_listener(self, addr, sub_headers):
        context = zmq.Context()
        subscriber = context.socket(zmq.SUB)
        subscriber.connect(f"tcp://{addr}")
        for i in sub_headers:
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
                    print(rcv)
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

        for i in self.sockets:
            th = threading.Thread(target=self.socket_listener, args=(i['addres'], i['subscribe']))
            th.start()
            thread_id.append(th)
        
        try:

            for i in thread_id:
                i.join()
        except KeyboardInterrupt as e:
            pass

        