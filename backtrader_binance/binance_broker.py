import datetime as dt
import binance.enums as be

from collections import defaultdict, deque

from backtrader.broker import BrokerBase
from backtrader.order import *
from backtrader.position import Position

class BinanceBroker(BrokerBase):
    _ORDER_TYPES = {
        Order.Limit: be.ORDER_TYPE_LIMIT,
        Order.Market: be.ORDER_TYPE_MARKET,
        Order.Stop: be.ORDER_TYPE_STOP_LOSS,
        Order.StopLimit: be.ORDER_TYPE_STOP_LOSS_LIMIT,
    }

    def __init__(self, store):
        super(BinanceBroker, self).__init__()

        self.notifs = deque()
        self.positions = defaultdict(Position)

        self.startingcash = self.cash = 0
        self.startingvalue = self.value = self.cash

        self.open_orders = dict()
    
        self._store = store
        self._store.binance_socket.start_user_socket(self._handle_user_socket_message)

    def start(self):
        pass

    def _execute_order(self, order, date, executed_size, executed_price, executed_value, executed_comm):
        order.execute(
            date,
            executed_size,
            executed_price,
            0, executed_value, executed_comm,
            0, 0.0, 0.0,
            0.0, 0.0,
            0, 0.0)
        pos = self.getposition(order.data, clone=False)
        pos.update(copysign(executed_size, order.size), executed_price)

    def _handle_user_socket_message(self, msg):
        """https://binance-docs.github.io/apidocs/spot/en/#payload-order-update"""
        # print(msg)
        # {'e': 'executionReport', 'E': 1707120960762, 's': 'ETHUSDT', 'c': 'oVoRofmTTXJCqnGNuvcuEu', 'S': 'BUY', 'o': 'MARKET', 'f': 'GTC', 'q': '0.00220000', 'p': '0.00000000', 'P': '0.00000000', 'F': '0.00000000', 'g': -1, 'C': '', 'x': 'NEW', 'X': 'NEW', 'r': 'NONE', 'i': 15859894465, 'l': '0.00000000', 'z': '0.00000000', 'L': '0.00000000', 'n': '0', 'N': None, 'T': 1707120960761, 't': -1, 'I': 33028455024, 'w': True, 'm': False, 'M': False, 'O': 1707120960761, 'Z': '0.00000000', 'Y': '0.00000000', 'Q': '0.00000000', 'W': 1707120960761, 'V': 'EXPIRE_MAKER'}

        # {'e': 'executionReport', 'E': 1707120960762, 's': 'ETHUSDT', 'c': 'oVoRofmTTXJCqnGNuvcuEu', 'S': 'BUY', 'o': 'MARKET', 'f': 'GTC', 'q': '0.00220000', 'p': '0.00000000', 'P': '0.00000000', 'F': '0.00000000', 'g': -1, 'C': '',
        # 'x': 'TRADE', 'X': 'FILLED', 'r': 'NONE', 'i': 15859894465, 'l': '0.00220000', 'z': '0.00220000', 'L': '2319.53000000', 'n': '0.00000220', 'N': 'ETH', 'T': 1707120960761, 't': 1297224255, 'I': 33028455025, 'w': False,
        # 'm': False, 'M': True, 'O': 1707120960761, 'Z': '5.10296600', 'Y': '5.10296600', 'Q': '0.00000000', 'W': 1707120960761, 'V': 'EXPIRE_MAKER'}
        if msg['e'] == 'executionReport':
            if msg['s'] in self._store.symbols:
                for o in self.open_orders:
                    if o.binance_order['orderId'] == msg['i']:
                        if msg['X'] in [be.ORDER_STATUS_FILLED, be.ORDER_STATUS_PARTIALLY_FILLED]:
                            _dt = dt.datetime.fromtimestamp(int(msg['T']) / 1000)
                            executed_size = float(msg['l'])
                            executed_price = float(msg['L'])
                            executed_value = float(msg['Z'])
                            executed_comm = float(msg['n'])
                            # print(_dt, executed_size, executed_price)
                            self._execute_order(o, _dt, executed_size, executed_price, executed_value, executed_comm)
                        self._set_order_status(o, msg['X'])

                        if o.status not in [Order.Accepted, Order.Partial]:
                            self.open_orders.remove(o)
                        self.notify(o)
        elif msg['e'] == 'error':
            raise msg
    
    def _set_order_status(self, order, binance_order_status):
        if binance_order_status == be.ORDER_STATUS_CANCELED:
            order.cancel()
        elif binance_order_status == be.ORDER_STATUS_EXPIRED:
            order.expire()
        elif binance_order_status == be.ORDER_STATUS_FILLED:
            order.completed()
        elif binance_order_status == be.ORDER_STATUS_PARTIALLY_FILLED:
            order.partial()
        elif binance_order_status == be.ORDER_STATUS_REJECTED:
            order.reject()

    def _submit(self, order):
        type = self._ORDER_TYPES.get(order.exectype, be.ORDER_TYPE_MARKET)
        symbol = order.data.symbol
        side = be.SIDE_BUY if order.ordtype == Order.Buy else be.SIDE_SELL
        size = abs(order.size) if order.size else None
        binance_order = self._store.create_order(symbol, side, type, size, order.price, **order.info)
        order.executed.remsize = float(binance_order['executedQty'])
        order.submit()
        # print(1111, binance_order)
        # 1111 {'symbol': 'ETHUSDT', 'orderId': 15860400971, 'orderListId': -1, 'clientOrderId': 'EO7lLPcYNZR8cNEg8AOEPb', 'transactTime': 1707124560731, 'price': '0.00000000', 'origQty': '0.00220000', 'executedQty': '0.00220000', 'cummulativeQuoteQty': '5.10356000', 'status': 'FILLED', 'timeInForce': 'GTC', 'type': 'MARKET', 'side': 'BUY', 'workingTime': 1707124560731, 'fills': [{'price': '2319.80000000', 'qty': '0.00220000', 'commission': '0.00000220', 'commissionAsset': 'ETH', 'tradeId': 1297261843}], 'selfTradePreventionMode': 'EXPIRE_MAKER'}
        # order = BinanceOrder(owner, data, exectype, binance_order)
        match binance_order['status']:
            case be.ORDER_STATUS_NEW:
                order.accept()
            case be.ORDER_STATUS_PARTIALLY_FILLED:
                self._process_order_trades(order, binance_order['transactTime'], binance_order['fills'])
                order.partial()
            case be.ORDER_STATUS_FILLED:
                self._process_order_trades(order, binance_order['transactTime'], binance_order['fills'])                
                order.completed()    
            case be.ORDER_STATUS_REJECTED:
                order.reject()
        
        self.notify(order)
        return order
    
    def _process_order_trades(self, order, transact_time, trades):
        comminfo = self.getcommissioninfo(order.data)
        position = self.positions[order.data]
        pprice_old = position.price
        # size = float(executedQty)
        date = dt.datetime.fromtimestamp(int(transact_time)/1000)
        
        for trade in trades:
            price = float(trade['price'])
            size = float(trade['qty']) if order.ordtype == Order.Buy else -float(trade['qty'])
            psize, pprice, opened, closed = position.pseudoupdate(size, price)
            openedvalue = opened * price if opened else 0.0
            openedcomm = float(trade['commission']) if opened else 0.0
            closedvalue = closed * price if closed else 0.0
            closedcomm = float(trade['commission']) if closed else 0.0
            margin = comminfo.margin
            pnl = comminfo.profitandloss(-closed, pprice_old, pprice)

            order.execute(date, size, price,
                closed, closedvalue, closedcomm,
                opened, openedvalue, openedcomm,
                margin, pnl,
                psize, pprice)
            order.addcomminfo(comminfo)

    def buy(self, owner, data, size, price=None, plimit=None,
            exectype=None, valid=None, tradeid=0, oco=None,
            trailamount=None, trailpercent=None,
            **kwargs):
        order = BuyOrder(owner=owner, data=data,
                         size=size, price=price, pricelimit=plimit,
                         exectype=exectype, valid=valid)
        order.addinfo(**kwargs)
        return self._submit(order)

    def cancel(self, order):
        order_id = order.binance_order['orderId']
        symbol = order.binance_order['symbol']
        self._store.cancel_order(symbol=symbol, order_id=order_id)
        
    def format_price(self, value):
        return self._store.format_price(value)

    def get_asset_balance(self, asset):
        return self._store.get_asset_balance(asset)

    def getcash(self):
        return self.cash

    def get_notification(self):
        if not self.notifs:
            return None

        return self.notifs.popleft()

    def getposition(self, data, clone=True):
        pos = self.positions[data._dataname]
        if clone:
            pos = pos.clone()
        return pos

    def getvalue(self, datas=None):
        self.value = self._store._value
        return self.value

    def notify(self, order):
        self.notifs.append(order)

    def sell(self, owner, data, size, price=None, plimit=None,
             exectype=None, valid=None, tradeid=0, oco=None,
             trailamount=None, trailpercent=None,
             **kwargs):
        order = SellOrder(owner=owner, data=data,
                         size=size, price=price, pricelimit=plimit,
                         exectype=exectype, valid=valid)
        order.addinfo(**kwargs)
        return self._submit(order)
        
    def set_cash(self, cash):
        '''Sets the cash parameter (alias: ``setcash``)'''
        self._store.get_balance()

        if cash <= self._store._cash:
            self.startingcash = self.cash = cash

    setcash = set_cash

    def add_cash(self, cash):
        self._store.get_balance()
        
        if self.cash + cash <= self._store._cash:
            self.cash += cash
