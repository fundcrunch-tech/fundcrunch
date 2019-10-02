from multiprocessing import Process, Queue
from threading import Thread

from fundcrunch import BinanceDriver, SocketEndppoint, EndpointsBinder


driver_output_queue = Queue()

driver_config = {"exchange": 'binance',
                 "pairs":[],
                 "mode":['order_book', 'trades', 'ohlc'],
                 "output": driver_output_queue
                }
        
public_drv = BinanceDriver( conf=driver_config, addr='0.0.0.0', port=9001 )
public_drv.start()

endpoint = SocketEndppoint(pull_pub=[{'pull':'0.0.0.0:9001', 'pub':'0.0.0.0:8010'}], endpoint='0.0.0.0:7001')
endpoint.start()


binder = EndpointsBinder(sockets=[{'addres':'0.0.0.0:7001', 'subscribe':['ob-binance-BTC_USDT'],}],
                         queues=[driver_output_queue]
                        )
binder.start()

try:
    while True:
        rcv = binder.output.get()
        print(rcv)



except KeyboardInterrupt as e:
    print('[feeder]','KeyboardInterrupt')



import os
import signal
if public_drv:
    os.kill(public_drv.pid, signal.SIGTERM)
    public_drv.join()

if endpoint.is_alive():
    endpoint.join()

if binder.is_alive():
    binder.join()







