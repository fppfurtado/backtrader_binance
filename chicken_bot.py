import os, datetime as dt
import logging.config, json, pathlib, atexit
import backtrader as bt
import database as db
from dotenv import load_dotenv
from backtrader_binance import BinanceStore

logger = logging.getLogger('trader')

def __main():
    setup_logging()

    load_dotenv(dotenv_path='.env', override=True)
    API_KEY = os.getenv('BINANCE_TESTNET_API_KEY')
    API_SECRET = os.getenv('BINANCE_TESTNET_API_SECRET')

    # base_asset = input('Base Asset: ').upper()
    base_asset = 'BTC'
    # quote_asset = input('Quote Asset: ').upper()
    quote_asset = 'USDT'
    symbol =  base_asset + quote_asset  # the ticker by which we will receive data in the format <CodeTickerBaseTicker>    

    store = BinanceStore(
        api_key=API_KEY,
        api_secret=API_SECRET,
        coin_target=quote_asset,
        testnet=True)  # Binance Storage
    
    cerebro = setup_engine(
        store=store,
        strategy_cls=ChickenStrategy,
        symbol=symbol, 
        timeframe=bt.TimeFrame.Minutes, 
        compression=1, 
        target_profit=0.0025, 
        live_bars=False
    )

    cerebro.run()

def setup_logging():
    config_file = pathlib.Path('logging.config.json')
    with open(config_file) as f_in:
        config = json.load(f_in)

    logging.config.dictConfig(config)
    queue_handler = logging.getHandlerByName("queue_handler")
    if queue_handler is not None:
        queue_handler.listener.start()
        atexit.register(queue_handler.listener.stop)

def setup_engine(store, strategy_cls, symbol, timeframe, compression, target_profit, live_bars):
    cerebro = bt.Cerebro(quicknotify=True)    
    
    if live_bars:
        broker = store.getbroker()
        cerebro.setbroker(broker)
    
    # Historical 1-minute bars for the last hour + new live bars / timeframe M1
    from_date = dt.datetime.now() - dt.timedelta(days=10)
    data = store.getdata(timeframe=timeframe, compression=compression, dataname=symbol, start_date=from_date, LiveBars=live_bars)
    
    cerebro.adddata(data)  # Adding data
    
    cerebro.addstrategy(
        strategy=strategy_cls, 
        store=store,
        target_profit=target_profit
    )
    
    return cerebro

class ChickenStrategy(bt.Strategy):
    logger = logging.getLogger('trader')

    params = (
        ('target_profit', 0.01),
    )

    def __init__(self, store):
        self.store = store
        self.asset = self.datas[0]
        self.symbol = self.asset._name
        self.closing_prices = self.datas[0].close
        self._executed_buy_orders_counter = 0
        self._executed_sell_orders_counter = 0
        self._total_profit = 0
        self._last_trade_date = None

    def next(self):
        self._log_debug('Position %.5f, Cash %.2f, Close %.2f, High %.2f, Low %.2f' % (self.position.size, self.broker.cash, self.closing_prices[0], self.asset.high[0], self.asset.low[0]))
        
        if self._has_buy_signal():
            if not self.position:
                self._open_trade_position()
                            
    def notify_order(self, order):
        match order.status:
            case bt.Order.Completed:
                log_message = f'{'BUY EXECUTED' if order.isbuy() else 'SELL EXECUTED'}({order.ref}, {order.executed.price:.2f}, {order.executed.size:.5f}) = {(order.executed.price * order.executed.size):.2f}'
                self._log_info(log_message)
                self._log_info(f'POSITION SIZE: {self.position.size:.5f}')
                
                if order.isbuy():
                    self._save_in_database(order)
                    self._executed_buy_orders_counter += 1
                    self._close_trade_position(order)
                else:
                    self._save_in_database(order)
                    self._executed_sell_orders_counter += 1
                    self._last_trade_date = bt.num2date(self.data.datetime[0]).date()
                    self._computes_trade_return(order)
            case bt.Order.Margin:
                self._log_info('ORDER MARGIN(%s)' % (order.ref))

            case bt.Order.Rejected:
                self._log_info('ORDER REJECTED(%s)' % (order.ref))

    def stop(self):
        self.logger.info(f'Finalizando {self.__class__.__name__}(target_profit = {self.p.target_profit}): Position {self.position.size:.5f}, Cash {self.broker.getcash():.2f}, Total Profit {round(self._total_profit, 2)}, Total Trades {self._executed_sell_orders_counter}, Last Trade Date {self._last_trade_date})')
             
    def _has_buy_signal(self) -> bool:
        if self._is_bullish(self._get_candle(0)):
            if self._is_bullish(self._get_candle(-1)):
                return True

    @staticmethod
    def _is_bullish(candle: []):
        ''' candle: [timestamp, open, high, low, close, ...] '''
        return candle[4] > candle[1]

    @staticmethod
    def _is_bearish(candle: []):
        ''' candle: [timestamp, open, high, low, close, ...] '''
        return candle[4] < candle[1]

    
    def _get_candle(self, index):
        return [
            self.asset.datetime[index],
            self.asset.open[index],
            self.asset.high[index],
            self.asset.low[index],
            self.asset.close[index]
        ]

    def _open_trade_position(self):
        stake = self.broker.getcash()
        current_price = self.closing_prices[0]
        order_size = stake/current_price
        min_order_size = float(self.store._min_order[self.symbol]) 
        
        if order_size >= min_order_size:
            self.buy(size=order_size)

    def _close_trade_position(self, order):
        sell_price = order.executed.price * (1 + self.p.target_profit)
        sell_value = sell_price * order.size
        min_order_value = float(self.store._min_order_in_target[self.symbol])
        
        if sell_value > min_order_value:
            self.sell(exectype=bt.Order.Limit, size=order.size, price=sell_price)

    def _computes_trade_return(self, order):
        sell_price = order.executed.price
        buy_price = order.executed.price/(1 + self.p.target_profit)
        order_return =  (sell_price - buy_price)*(-order.size)
        self._log_info(f'TRADE RETURN = {round(order_return, 2)}')
        self._total_profit = self._total_profit + order_return
        self._log_info(f'TOTAL PROFIT/LOSS = {round(self._total_profit, 2)}')

    def _save_in_database(self, order):
        db.save_trade(
            bt.num2date(order.executed.dt),
            'BUY' if order.ordtype == 0 else 'SELL',
            self.symbol,
            order.executed.price,
            order.size,
            order.executed.comm,
            'BINANCE'
        )

    def _log_debug(self, txt, dt=None, carriage_return=False):
        self._log('d', txt, dt, carriage_return)

    def _log_info(self, txt, dt=None, carriage_return=False):
        self._log('i', txt, dt, carriage_return)

    def _log(self, level, txt, dt=None, carriage_return=False):
        '''Logging function for the strategy'''
        dt = dt or self.datas[0].datetime.datetime()
        if level == 'i':
            if not carriage_return:
                # print('%s, %s' % (dt, txt))
                self.logger.info('%s, %s' % (dt, txt))
            else:
                # print('\r%s, %s' % (dt, txt))
                self.logger.info('\r%s, %s' % (dt, txt))
        elif level == 'd':
            if not carriage_return:
                # print('%s, %s' % (dt, txt))
                self.logger.debug('%s, %s' % (dt, txt))
            else:
                # print('\r%s, %s' % (dt, txt))
                self.logger.debug('\r%s, %s' % (dt, txt))


if __name__ == '__main__':
    __main()