import zmq
import time
import sys, json
from random import randrange, randint
import random
from  multiprocessing import Process
import binascii
import gzip
import pprint


    
class PushPull(Process):

    def __init__(self, name, pull_address, push_address):
        Process.__init__(self)
        self.name = f"[{name}]"
        self.pull_address = f"tcp://{pull_address}"
        self.push_address = f"tcp://{push_address}"
    
    def run(self):

        nonce = 0
        
        context = zmq.Context()
        puller = context.socket(zmq.PULL)
        puller.bind(self.pull_address)
        print(self.name, '[PULL]', self.pull_address )

        publisher = context.socket(zmq.PUB)
        publisher.bind(self.push_address)
        print(self.name, '[PUB]', self.push_address )

        try:
            while True:
                try:
                    s = puller.recv()#_json()
                    s = json.loads(s)
                    
                except (zmq.ZMQError, ValueError) as e:
                    print('ERROR', e, )
                    #puller.close()
                    #context.term()
                    #sys.exit()

                else:
                    s['feeder_nonce'] = nonce
                    if s['message'] in ['ob', 'trade', 'ohlc']:
                        sub_str = s['message'] + '-' + s['exchange'] + '-' + s['symbol'].replace('/','_') + '@'
                    elif s['message'] in ['balance', 'order_update']:
                        sub_str = f"{s['message']}-{s['exchange']}-{s['auth']}@"
                    elif s['message'] in ['active_symbols']:
                        sub_str = 'info-'+ s['exchange'] + '@'

                    publisher.send_string( sub_str + json.dumps(s) )
                    nonce += 1
                
        
        except KeyboardInterrupt as e:
            print(self.name,  'KeyboardInterrupt')
            puller.close()
            #context.term()
            #sys.exit()




class SocketEndppoint(Process):
    drv_pid = []
    pull_pub_pid = []

    def __init__(self, pull_pub, endpoint):
        Process.__init__(self)
        self.name = "[SocketEndpoint]"
        self.endpoint = endpoint
        self.pull_push = pull_pub

        for i in pull_pub:
            pp = PushPull('push_pull', i['pull'], i['pub'])
            pp.start()
            self.pull_pub_pid.append(pp)

            
    def __del__(self):
        for i in self.pull_pub_pid:
            i.join()
    
    def run(self):
        context = zmq.Context()

        xsub = context.socket(zmq.XSUB)

        for i in self.pull_push:
            sourceaddress = f"tcp://{i['pub']}"
            xsub.connect(sourceaddress)
            print(self.name, '[XSUB]', sourceaddress)
        
        bindaddress = f"tcp://{self.endpoint}"
        xpub = context.socket(zmq.XPUB)
        try:
            xpub.bind(bindaddress)
        except Exception as e:
            print(self.name, bindaddress, e)
        else:
            print(self.name, '[XPUB]', bindaddress)

        time.sleep(2)
        
        poller = zmq.Poller()
        poller.register(xsub, zmq.POLLIN)
        poller.register(xpub, zmq.POLLIN)

        
        while True:

            try:
            
                socks = dict(poller.poll(100))
                
            except KeyboardInterrupt as e:
                print(self.name, 'KeyboardInterrupt')
                xsub.close()
                xpub.close()
                context.term()
                sys.exit()

            if xsub in socks:
                message = xsub.recv_string()
                
                #topic, payload = message.split('@',1)
                # s_out = gzip.compress( payload.encode('utf-8') )
                # topic += '@'
                # xpub.send ( topic.encode('utf-8') + s_out   )
                
                xpub.send_string( message   )

            
            if xpub in socks:
                message = xpub.recv_string()
                xsub.send_string(message)

            

        xsub.close()
        xpub.close()
        context.term()



