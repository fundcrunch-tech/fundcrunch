import asyncio
import os
import sys
import uuid
import threading, queue
from time import sleep
import datetime, time
import pprint
import copy

import sys
sys.path.append('..')

#from owncommon.messages import Order, PairBalance, Tick, Balance, Error, OrderBook


pp = pprint.PrettyPrinter(indent=4)

root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(root + '/python')

import ccxt.async_support as ccxt  # noqa: E402

TIMEOUT = 5  # seconds
TICK_TIMER = 15
OD_TICK_TIMER = 10
BALANCE_TICK_TIMER = 30
TRADE_TICK_TIMER = 5
INFO_TIMER = 30

class CcxtDriver:

    def __init__(self, loop, config):

        self.ob_constant = OD_TICK_TIMER
        self.bl_constant = BALANCE_TICK_TIMER
        self.trade_constant = TRADE_TICK_TIMER

        self.stop_tick_time = datetime.datetime.now() + datetime.timedelta(seconds=TICK_TIMER)
        self.orderbook_tick_time = datetime.datetime.now() + datetime.timedelta(seconds=self.ob_constant)
        self.balance_tick_time = datetime.datetime.now() + datetime.timedelta(seconds=self.bl_constant)
        self.trade_tick_time = datetime.datetime.now() + datetime.timedelta(seconds=self.trade_constant)
        self.info_tick_time = datetime.datetime.now() +  datetime.timedelta(seconds=INFO_TIMER)



        self.config = config
        self.orderbook_count = 0
        self.pair_info = dict()
        self.logger = None
        if 'logger' in self.config.keys():
            self.logger = self.config['logger']
        

        self.exhange = config['exchange']
        self.is_auth = False
        self.name = '[ccxt %s]' % self.exhange
        self.pair_list = set()

        if self.exhange == 'liqui':
            self.ob_constant = 30
            self.bl_constant = 60

        self.ccxt_it_queue = self.config['ccxt_in_queue']
        self.ccxt_out_queue = self.config['ccxt_out_queue']

        self.pair_list = self.config['pairs']


        # for i in self.config['pairs']:
        #     i['balance_tick'] = True
        #     self.pair_list.add( i['name'] )

        auth = {}
        if 'auth' in self.config.keys():
            auth = self.config['auth']
            self.is_auth = True
            self.name = '[ccxt %s %s*]' % (self.exhange, auth['apiKey'][:4])

        asyncio.set_event_loop(loop)


        if self.exhange == 'hitbtc':
            loop.create_task(self.run_loop(ccxt.hitbtc( auth )))
        elif self.exhange == 'coinmarketcap':
            loop.create_task(self.run_loop(ccxt.coinmarketcap()))
        elif self.exhange == 'binance':
            loop.create_task(self.run_loop(ccxt.binance( auth )))
        elif self.exhange == 'bitmex':
            loop.create_task(self.run_loop(ccxt.bitmex(auth)))
        elif self.exhange == 'huobipro':
            loop.create_task(self.run_loop(ccxt.huobipro()))
        elif self.exhange == 'liqui':
            loop.create_task(self.run_loop(ccxt.liqui(auth)))
        elif self.exhange == 'bitfinex2':
            loop.create_task(self.run_loop(ccxt.bitfinex2( auth )))
        elif self.exhange == 'bitfinex':
            loop.create_task(self.run_loop(ccxt.bitfinex( auth )))
        elif self.exhange == 'okex':
            loop.create_task(self.run_loop(ccxt.okex( auth )))
        elif self.exhange == 'kucoin':
            loop.create_task(self.run_loop(ccxt.kucoin( auth )))
        elif self.exhange == 'bittrex':
            loop.create_task(self.run_loop(ccxt.bittrex( auth )))
        elif self.exhange == 'qryptos':
            loop.create_task(self.run_loop(ccxt.qryptos( auth )))
        elif self.exhange == 'kraken':
            loop.create_task(self.run_loop(ccxt.kraken( auth )))



        loop.run_forever()



    def get_active_symbols(self, exchange):
        return [symbol for symbol in exchange.symbols if self.is_active_symbol(exchange, symbol)]


    def is_active_symbol(self, exchange, symbol):

        return ('.' not in symbol) and (('active' not in exchange.markets[symbol]) or (exchange.markets[symbol]['active']))


    async def fetch_ticker(self, exchange, symbol):

        order_time = datetime.datetime.today().replace(hour=13, minute=40)
        order_time = time.mktime(order_time.timetuple())

        to_sen_message = None
        try:
            ticker = await exchange.fetch_ticker(symbol)
        except Exception as e:
            #print('[ccxt] ERROR',exchange.id, symbol, 'tick', e)
            print(f'[ccxt] {exchange.id} {symbol} {e}')
            to_sen_message = {'message': 'error', 'exchange':exchange.id, 'type':'tick', 'symbol':symbol, 'error': e}
        else:
            to_sen_message = {'message': 'tick', 'exchange':exchange.id, 'symbol':symbol, 'tick': ticker}


        if to_sen_message:
            self.ccxt_out_queue.put(to_sen_message)

        # createOrder (symbol, type, side, amount[, price[, params]])
        # if exchange.id in ['liqui','bittrex','kucoin']:
        #     print(self.name, exchange.id)
        #     #exchange.createOrder(symbol=symbol, side='buy', amount=order.amount)
        #     exchange.create_limit_buy_order(symbol, 1, 5000))
        # #     exchange.createOrder(symbol, )

        # orders = await exchange.fetch_closed_orders(symbol=symbol)
        # print(orders)

        #orders = await exchange.fetch_open_orders(symbol)
        #print(orders)
        #conf['extern_exchange']['tick'].put(ticker)



        return ticker
    
    
    async def cancel_order(self, exchange, order, return_queue):
        try:
            await exchange.cancel_order( str(order['id']), order['symbol'] )
        except Exception as e:
            print(f"{self.name} {exchange.id} {order['symbol']} cancel_order({order['id']})   {e}")


    async def open_order(self, exchange, order, return_queue):
        
        #print(self.name, order['symbol'], order['side'], order['volume'], order['price'])

        order['price'] = exchange.price_to_precision(order['symbol'], float(order['price']))
        #order['volume'] = exchange.amount_to_lots(order['symbol'], order['volume'])
       

        print(f"{self.name} {order['symbol']} {order['side']} {order['volume']} {order['price']}")

        if 'lot' in self.pair_info[ order['symbol'] ]:

            lot = self.pair_info[order['symbol']]['lot']

            if lot != 1.0:
                old_amount =  order['volume']

                a = order['volume'] / lot
                first_part = str(a).split('.')[0]

                order['volume'] = int(first_part)*lot
                order['volume'] = round(order['volume'], 8)

                # tail = order.amount % self.pair_info[order.symbol]['lot']
                # order.amount = order.amount - tail

                print(self.name, order['symbol'], old_amount, 'rounded to', order['volume'], 'by', self.pair_info[ order['symbol']  ]['lot'] )
                print(self.name, order['symbol'], order['side'] , order['volume'], order.price )

        try:
            if exchange.id in ['liqui','bittrex', 'kucoin'] or order['type']=='limit':
                # createOrder (symbol, type, side, amount[, price[, params]])
                if order['side'] == 'buy':
                    order_callback = await exchange.create_limit_buy_order(order['symbol'], order['volume'], order['price'])
                elif order['side'] == 'sell':
                    order_callback = await exchange.create_limit_sell_order(order['symbol'], order['volume'], order['price'])

            elif order['type']=='stop_limit':
                order['limit_price'] = exchange.price_to_precision(order['symbol'], float(order['limit_price']))
                order_callback = await exchange.createOrder(order['symbol'], 
                                                            'stop_loss_limit', 
                                                            order['side'], 
                                                            order['volume'], 
                                                            order['limit_price'], 
                                                            { "stopPrice": order['price'],
                                                              "newClientOrderId": order['uuid'] })
                

            
            elif  exchange.id == 'okex':
                if order.side == 'buy':
                    order_callback = await exchange.createMarketBuyOrder (order['symbol'],  None, {'cost': order['volume'] * order['price'] * 1.1})
                elif order.side == 'sell':
                    order_callback = await exchange.createMarketSellOrder (order['symbol'],  order['volume'])

            else:
                if order['side'] == 'buy':
                    order_callback = await exchange.createMarketBuyOrder(symbol=order['symbol'], amount=order['volume'])
                elif order['side'] == 'sell':
                    order_callback = await exchange.createMarketSellOrder(symbol=order['symbol'], amount=order['volume'])



        except Exception as e:
            print(f"{self.name} {exchange.id} {order['symbol']} {e}")
            order['status'] = 'error'
            order['e'] = e
            self.ccxt_out_queue.put({'message': 'on_open_order', 'order': order})
        else:
            # print(self.name, '[OK]', exchange.id, order['symbol'], order_callback['side'], 
            #       order_callback['id'], order['price'], 'vs', order_callback['price'], 
            #       order_callback['status'], order_callback['filled'], order_callback['cost'] )

            print(f"{self.name} {order['symbol']} {order_callback['side']} "\
                             f"{order['price']} vs {order_callback['price']} "\
                             f"{order_callback['status']} {order_callback['filled']} {order_callback['cost']}")
            
            order['id'] = order_callback['id']
            order['exec_price'] = order_callback['price']
            order['status'] = order_callback['status']
            order['filled'] = order_callback['filled']
            order['cost'] = order_callback['cost']

            self.ccxt_out_queue.put({'message':'on_open_order', 'order': order})

        return order

    async def fetch_balance_one(self, exchange, coin):
        try:

            balance = await exchange.fetch_balance()

        except Exception as e:
            print('[ccxt]', exchange.id, 'balance', e)
            to_sen_message = {'message': 'error', 'type': 'balcance', 'exchange': exchange.id, 'symbol': 'x',
                              'error': e}
        else:
            if coin in balance.keys():
                to_sen_message = {'message': 'bl', 'account': self.config['auth']['apiKey'],
                                  'exchange': exchange.id, 'symbol': coin, 'balance': balance[coin]}
                self.ccxt_out_queue.put(to_sen_message)



    async def fetch_balance(self, exchange, symbol):
        #print(self.name, exchange.id, symbol, 'balance.....')

        to_sen_message = None
        if self.is_auth:

            try:
                balance = await exchange.fetch_balance()

            except Exception as e:
                print('[ccxt]', exchange.id, symbol, 'balance', e)
                to_sen_message = {'message': 'error', 'type':'balcance', 'exchange':exchange.id, 'symbol':symbol, 'error': e}
            else:
                for s in symbol:
                    t, b = s.split('/')
                    if t in balance.keys():
                        to_sen_message = {'message': 'bl', 'account':self.config['auth']['apiKey'][:4]+'*', 'exchange':exchange.id, 'symbol':t, 'balance': balance[t]}
                        self.ccxt_out_queue.put(to_sen_message)

                    if b in balance.keys():
                        to_sen_message = {'message': 'bl', 'account':self.config['auth']['apiKey'][:4]+'*', 'exchange':exchange.id, 'symbol':b, 'balance': balance[b]}
                        self.ccxt_out_queue.put(to_sen_message)
                    
                    self.ccxt_out_queue.put(to_sen_message)

        return {}

    async def fetch_orderbook(self, exchange, symbol):

        to_sen_message = {}
        try:
            orderbook = await exchange.fetch_order_book(symbol, 20)
        except Exception as e:
            print('[ccxt] ERROR',exchange.id, symbol, 'order_book', e)
            to_sen_message = {'message': 'error', 'exchange':exchange.id, 'symbol':symbol, 'type':'order_book', 'error':  e  }
        else:
            to_sen_message = {'message':'ob', 'exchange':exchange.id, 'symbol':symbol, 'order_book': orderbook}

        if to_sen_message:
            self.ccxt_out_queue.put(to_sen_message)

        # if 'kraken' in exchange.id:
        #     print(self.name, symbol, orderbook)

        return orderbook

    async def fetch_trades(self, exchange, symbol):

        try:
            sincet = int(time.time())
            trades = await exchange.fetch_trades ( symbol, since=sincet  )

        except Exception as e:
            print(self.name, 'ERROR',exchange.id, symbol, 'trades', e)
        else:
            # {'id': '99476041',
            #  'order': None,
            #  'timestamp': 1528197205000,
            #  'datetime': '2018-06-05T11:13:25.000Z',
            #  'symbol': 'VEN/ETH',
            #  'type': 'limit',
            #  'side': 'sell',
            #  'price': 0.00650576,
            #  'amount': 26.28332,
            #  'fee': {'type': 'taker', 'currency': 'ETH', 'rate': 0.0025, 'cost': 0.000427482429808},
            #  'info': {'type': 'ask', 'price': 0.00650576, 'amount': 26.28332, 'tid': 99476041, 'timestamp': 1528197205}},
            for i in trades:
                print(self.name, 'trade', i['symbol'], i['datetime'])

            #print(trades)
            pass

        return trades


    async def run_loop(self, exchange):

        await exchange.load_markets()
        sleep_in_pairs = exchange.rateLimit / 1000

        #print(exchange.has)

        #symbols_to_load = self.get_active_symbols(exchange)

        self.ccxt_out_queue.put( {'message': 'active_symbols',
                                  'exchange':exchange.id,
                                  'active_symbols': self.get_active_symbols(exchange) }  )

        #matching = [s for s in symbols_to_load if "AION" in s]
        #print(matching)

        symbols_to_load = list(self.pair_list)
        print(f"{self.name} {self.config['mode']} {len(symbols_to_load)} symbols, rate_limit {sleep_in_pairs} ")

        for i in self.pair_list:
            self.pair_info[i] = exchange.markets[i]

        for k,v in self.pair_info.items():
            if 'lot' in v.keys():
                print(self.name, exchange.id, k, 'order amount round to', v['lot'])

        while True:

            if self.is_auth:
                '''loop for any symbol to find queue'''               
                try:
                    message = self.config['ccxt_in_queue'].get_nowait()
                except queue.Empty:
                    pass
                else:
                    if message['command'] == 'open_order':                        
                        await self.open_order(exchange, message['order'], self.config['ccxt_out_queue'])
                    elif message['command'] == 'cancel_order':
                        await self.cancel_order(exchange, message['order'], self.config['ccxt_out_queue'])
                    elif message['command'] == 'get_balance':
                        await self.fetch_balance_one(exchange, message['coin'])


                    #sleep(sleep_in_pairs)    
                    #self.ccxt_out_queue.put(message['order'])


            # =======================
            # for Tick
            # =======================
            # if 'tick' in self.config['mode']:
            #     if datetime.datetime.now() > self.stop_tick_time:
            #         self.stop_tick_time = datetime.datetime.now() + datetime.timedelta(seconds=TICK_TIMER)

            #         input_coroutines = [self.fetch_ticker(exchange, symbol) for symbol in symbols_to_load]
            #         tickers = await asyncio.gather(*input_coroutines, return_exceptions=True)

            #         for ticker, symbol in zip(tickers, symbols_to_load):
            #             if not isinstance(ticker, dict):
            #                 print(self.name, 'tick', exchange.name, symbol, ticker)

            if self.is_auth and 'balance' in self.config['mode']:
                # =======================
                # for Balance
                # =======================
                if datetime.datetime.now() >  self.balance_tick_time:
                    self.balance_tick_time = datetime.datetime.now() + datetime.timedelta(seconds=self.bl_constant)

                    bl_coroutines = []
                    #for si in  symbols_to_load:
                    bl_coroutines.append( self.fetch_balance(exchange, symbols_to_load) )
                    #sleep(sleep_in_pairs)

                    ob = await asyncio.gather(*bl_coroutines, return_exceptions=True)

                    # bl_coroutines = [self.fetch_balance(exchange, symbol) for symbol in symbols_to_load]
                    # bl = await asyncio.gather(*bl_coroutines, return_exceptions=True)


            #=======================
            #for OrderBook
            #=======================

            if 'order_book' in self.config['mode']:

                if datetime.datetime.now() >  self.orderbook_tick_time:
                    self.orderbook_tick_time = datetime.datetime.now() + datetime.timedelta(seconds=self.ob_constant)

                    ob_coroutines = []
                    for si in  symbols_to_load:
                        ob_coroutines.append( self.fetch_orderbook(exchange, si))
                        #sleep(sleep_in_pairs)

                    ob = await asyncio.gather(*ob_coroutines, return_exceptions=True)

                    # ob_coroutines = [self.fetch_orderbook(exchange, symbol) for symbol in symbols_to_load]
                    # ob = await asyncio.gather(*ob_coroutines, return_exceptions=True)
                    # for ticker, symbol in zip(ob, symbols_to_load):
                    #     if not isinstance(ob, dict):
                    #         print(self.name, 'ERROR order_book', exchange.name, symbol)

            # ===========================
            # Trades
            # ===========================
            # if 'trades' in self.config['mode']:
            #     if datetime.datetime.now() >  self.trade_tick_time:
            #         self.trade_tick_time = datetime.datetime.now() + datetime.timedelta(seconds=self.trade_constant)
            #         trade_coroutines = []
            #         for si in  symbols_to_load:
            #             trade_coroutines.append( self.fetch_trades(exchange, si) )
            #             sleep(sleep_in_pairs)

            #         td = await asyncio.gather(*trade_coroutines, return_exceptions=True)

            # ===========================================
            # info Tick
            # ===========================================
            if datetime.datetime.now() >  self.info_tick_time:
                self.info_tick_time = datetime.datetime.now() + datetime.timedelta(seconds=INFO_TIMER)

                self.ccxt_out_queue.put( {'message': 'active_symbols',
                                          'exchange':exchange.id,
                                          'active_symbols': self.get_active_symbols(exchange) }  )


            sleep(0.01)
