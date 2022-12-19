'''
Copyright (C) 2017-2022 Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from decimal import Decimal
import logging
import time
from typing import Dict, Tuple
import hmac
import base64
import hashlib

from yapic import json

from cryptofeed.defines import ASK, BID, BUY, CANDLES, KUCOIN, L2_BOOK, LIMIT, MARKET, SELL, TICKER, TRADES, ORDER_INFO, BALANCES
from cryptofeed.feed import Feed
from cryptofeed.util.time import timedelta_str_to_sec
from cryptofeed.symbols import Symbol
from cryptofeed.connection import AsyncConnection, RestEndpoint, Routes, WebsocketEndpoint
from cryptofeed.types import OrderBook, Trade, Ticker, Candle, OrderInfo, Balance


LOG = logging.getLogger('feedhandler')


class KuCoin(Feed):
    id = KUCOIN
    websocket_endpoints = None
    rest_endpoints = [
        RestEndpoint(
            'https://api.kucoin.com',
            routes=Routes(
                '/api/v1/symbols',
                l2book='/api/v3/market/orderbook/level2?symbol={}',
                balances='/api/v1/accounts',
                ws_private_channel='/api/v1/bullet-private'
            )
        )]
    valid_candle_intervals = {'1m', '3m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '1w'}
    candle_interval_map = {'1m': '1min', '3m': '3min', '15m': '15min', '30m': '30min', '1h': '1hour', '2h': '2hour', '4h': '4hour', '6h': '6hour', '8h': '8hour', '12h': '12hour', '1d': '1day', '1w': '1week'}
    websocket_channels = {
        L2_BOOK: '/market/level2',
        TRADES: '/market/match',
        TICKER: '/market/ticker',
        CANDLES: '/market/candles',
        ORDER_INFO: '/spotMarket/tradeOrders',
        BALANCES: '/account/balance'
    }

    @classmethod
    def is_authenticated_channel(cls, channel: str) -> bool:
        return channel in (L2_BOOK)

    @classmethod
    def _parse_symbol_data(cls, data: dict) -> Tuple[Dict, Dict]:
        ret = {}
        info = {'tick_size': {}, 'instrument_type': {}}
        for symbol in data['data']:
            if not symbol['enableTrading']:
                continue
            s = Symbol(symbol['baseCurrency'], symbol['quoteCurrency'])
            info['tick_size'][s.normalized] = symbol['priceIncrement']
            ret[s.normalized] = symbol['symbol']
            info['instrument_type'][s.normalized] = s.type
        return ret, info

    def __init__(self, **kwargs):
        self._load_config(kwargs.get("config"))
        str_to_sign = "POST" + self.rest_endpoints[0].routes.ws_private_channel
        headers = self.generate_token(str_to_sign)
        address_info = self.http_sync.write(self.rest_endpoints[0].route('ws_private_channel', self.sandbox), json=True, headers=headers)
        token = address_info['data']['token']
        address = address_info['data']['instanceServers'][0]['endpoint']
        address = f"{address}?token={token}"
        self.websocket_endpoints = [WebsocketEndpoint(address, options={'ping_interval': address_info['data']['instanceServers'][0]['pingInterval'] / 2000})]
        super().__init__(**kwargs)
        if any([len(self.subscription[chan]) > 300 for chan in self.subscription]):
            raise ValueError("Kucoin has a limit of 300 symbols per connection")
        self.__reset()

    def __reset(self):
        self._l2_book = {}
        self.seq_no = {}

    async def _candles(self, msg: dict, symbol: str, timestamp: float):
        """
        {
            'data': {
                'symbol': 'BTC-USDT',
                'candles': ['1619196960', '49885.4', '49821', '49890.5', '49821', '2.60137567', '129722.909001802'],
                'time': 1619196997007846442
            },
            'subject': 'trade.candles.update',
            'topic': '/market/candles:BTC-USDT_1min',
            'type': 'message'
        }
        """
        symbol, interval = symbol.split("_")
        interval = self.normalize_candle_interval[interval]
        start, open, close, high, low, vol, _ = msg['data']['candles']
        end = int(start) + timedelta_str_to_sec(interval) - 1
        c = Candle(
            self.id,
            symbol,
            int(start),
            end,
            interval,
            None,
            Decimal(open),
            Decimal(close),
            Decimal(high),
            Decimal(low),
            Decimal(vol),
            None,
            msg['data']['time'] / 1000000000,
            raw=msg
        )
        await self.callback(CANDLES, c, timestamp)

    async def _ticker(self, msg: dict, symbol: str, timestamp: float):
        """
        {
            "type":"message",
            "topic":"/market/ticker:BTC-USDT",
            "subject":"trade.ticker",
            "data":{

                "sequence":"1545896668986", // Sequence number
                "price":"0.08",             // Last traded price
                "size":"0.011",             //  Last traded amount
                "bestAsk":"0.08",          // Best ask price
                "bestAskSize":"0.18",      // Best ask size
                "bestBid":"0.049",         // Best bid price
                "bestBidSize":"0.036"     // Best bid size
            }
        }
        """
        t = Ticker(self.id, symbol, Decimal(msg['data']['bestBid']), Decimal(msg['data']['bestAsk']), None, raw=msg)
        await self.callback(TICKER, t, timestamp)

    async def _trades(self, msg: dict, symbol: str, timestamp: float):
        """
        {
            "type":"message",
            "topic":"/market/match:BTC-USDT",
            "subject":"trade.l3match",
            "data":{

                "sequence":"1545896669145",
                "type":"match",
                "symbol":"BTC-USDT",
                "side":"buy",
                "price":"0.08200000000000000000",
                "size":"0.01022222000000000000",
                "tradeId":"5c24c5da03aa673885cd67aa",
                "takerOrderId":"5c24c5d903aa6772d55b371e",
                "makerOrderId":"5c2187d003aa677bd09d5c93",
                "time":"1545913818099033203"
            }
        }
        """
        t = Trade(
            self.id,
            symbol,
            BUY if msg['data']['side'] == 'buy' else SELL,
            Decimal(msg['data']['size']),
            Decimal(msg['data']['price']),
            float(msg['data']['time']) / 1000000000,
            id=msg['data']['tradeId'],
            raw=msg
        )
        await self.callback(TRADES, t, timestamp)

    async def _balance_snapshot(self):
        str_to_sign = "GET" + self.rest_endpoints[0].routes.balances
        headers = self.generate_token(str_to_sign)
        ret = await self.http_conn.read(self.rest_endpoints[0].route('balances', self.sandbox), header=headers)
        result = json.loads(ret, parse_float=Decimal)
        data = result["data"]

        for balance in data:
            b = Balance(
                self.id,
                balance['currency'],
                Decimal(balance['available']),
                Decimal(balance['balance']) - Decimal(balance['available']),
                timestamp=int(time.time()),
                raw=balance)
            await self.callback(BALANCES, b, int(time.time()))

    async def _balance(self, msg: dict, timestamp: float):
        """
        {
            "type": "message",
            "topic": "/account/balance",
            "subject": "account.balance",
            "channelType":"private",
            "data": {
                "total": "88", // total balance
                "available": "88", // available balance
                "availableChange": "88", // the change of available balance
                "currency": "KCS", // currency
                "hold": "0", // hold amount
                "holdChange": "0", // the change of hold balance
                "relationEvent": "trade.setted", //relation event
                "relationEventId": "5c21e80303aa677bd09d7dff", // relation event id
                "relationContext": {
                    "symbol":"BTC-USDT",
                    "tradeId":"5e6a5dca9e16882a7d83b7a4", // the trade Id when order is executed
                    "orderId":"5ea10479415e2f0009949d54"
                },  // the context of trade event
                "time": "1545743136994" // timestamp
            }
        }
        """
        data = msg["data"]
        b = Balance(
            self.id,
            data['currency'],
            Decimal(data['available']),
            Decimal(data['total']) - Decimal(data['available']),
            timestamp=data['time'],
            raw=data)
        await self.callback(BALANCES, b, timestamp)

    async def _order_update(self, msg: dict, timestamp: float):
        """
        {
            "type":"message",
            "topic":"/spotMarket/tradeOrders",
            "subject":"orderChange",
            "channelType":"private",
            "data":{
                "symbol":"KCS-USDT",
                "orderType":"limit",
                "side":"buy",
                "orderId":"5efab07953bdea00089965d2",
                "type":"open",
                "orderTime":1593487481683297666,
                "size":"0.1",
                "filledSize":"0",
                "price":"0.937",
                "clientOid":"1593487481000906",
                "remainSize":"0.1",
                "status":"open",
                "ts":1593487481683297666
            }
        }
        """
        data = msg["data"]
        oi = OrderInfo(
            self.id,
            self.exchange_symbol_to_std_symbol(data['symbol']),
            data['orderId'],
            SELL if data['side'].lower() == 'sell' else BUY,
            self.normalize_order_status(data),
            LIMIT if data['orderType'].lower() == 'limit' else MARKET if data['orderType'].lower() == 'market' else None,
            Decimal(data['price']),
            Decimal(data['size']),
            Decimal(data['remainSize']),
            data["ts"] / 1000,
            raw=data
        )
        await self.callback(ORDER_INFO, oi, timestamp)

    @staticmethod
    def normalize_order_status(order: dict):
        state = None
        event_type = order.get("type")

        if event_type == "filled":
            state = "FILLED"
        if event_type == "canceled":
            state = "CANCELED"
        if event_type == "open":
            state = "NEW"
        return state

    def generate_token(self, str_to_sign: str) -> dict:
        # https://docs.kucoin.com/#authentication

        # Now required to pass timestamp with string to sign. Timestamp should exactly match header timestamp
        now = str(int(time.time() * 1000))
        str_to_sign = now + str_to_sign
        signature = base64.b64encode(hmac.new(self.key_secret.encode('utf-8'), str_to_sign.encode('utf-8'), hashlib.sha256).digest())
        # Passphrase must now be encrypted by key_secret
        passphrase = base64.b64encode(hmac.new(self.key_secret.encode('utf-8'), self.key_passphrase.encode('utf-8'), hashlib.sha256).digest())

        # API key version is currently 2 (whereas API version is anywhere from 1-3 ¯\_(ツ)_/¯)
        header = {
            "KC-API-KEY": self.key_id,
            "KC-API-SIGN": signature.decode(),
            "KC-API-TIMESTAMP": now,
            "KC-API-PASSPHRASE": passphrase.decode(),
            "KC-API-KEY-VERSION": "2"
        }
        return header

    async def _snapshot(self, symbol: str):
        str_to_sign = "GET" + self.rest_endpoints[0].routes.l2book.format(symbol)
        headers = self.generate_token(str_to_sign)
        data = await self.http_conn.read(self.rest_endpoints[0].route('l2book', self.sandbox).format(symbol), header=headers)
        timestamp = time.time()
        data = json.loads(data, parse_float=Decimal)
        data = data['data']
        self.seq_no[symbol] = int(data['sequence'])
        bids = {Decimal(price): Decimal(amount) for price, amount in data['bids']}
        asks = {Decimal(price): Decimal(amount) for price, amount in data['asks']}
        self._l2_book[symbol] = OrderBook(self.id, symbol, max_depth=self.max_depth, bids=bids, asks=asks)

        await self.book_callback(L2_BOOK, self._l2_book[symbol], timestamp, timestamp=timestamp, raw=data, sequence_number=int(data['sequence']))

    async def _process_l2_book(self, msg: dict, symbol: str, timestamp: float):
        """
        {
            'data': {
                'sequenceStart': 1615591136351,
                'symbol': 'BTC-USDT',
                'changes': {
                    'asks': [],
                    'bids': [['49746.9', '0.1488295', '1615591136351']]
                },
                'sequenceEnd': 1615591136351,
                "time": 1663747970273 //milliseconds
            },
            'subject': 'trade.l2update',
            'topic': '/market/level2:BTC-USDT',
            'type': 'message'
        }
        """
        data = msg['data']
        sequence = data['sequenceStart']
        if symbol not in self._l2_book or sequence > self.seq_no[symbol] + 1:
            if symbol in self.seq_no and sequence > self.seq_no[symbol] + 1:
                LOG.warning("%s: Missing book update detected, resetting book", self.id)
            await self._snapshot(symbol)

        data = msg['data']
        if sequence < self.seq_no[symbol]:
            return

        self.seq_no[symbol] = data['sequenceEnd']

        if 'time' in data:
            ts = data['time'] / 1000
        else:
            LOG.debug("No timestamp data returned by Kucoin websocket.")
            ts = None
        delta = {BID: [], ASK: []}
        for s, side in (('bids', BID), ('asks', ASK)):
            for update in data['changes'][s]:
                price = Decimal(update[0])
                amount = Decimal(update[1])

                if amount == 0:
                    if price in self._l2_book[symbol].book[side]:
                        del self._l2_book[symbol].book[side][price]
                        delta[side].append((price, amount))
                else:
                    self._l2_book[symbol].book[side][price] = amount
                    delta[side].append((price, amount))

        await self.book_callback(L2_BOOK, self._l2_book[symbol], timestamp, timestamp=ts, delta=delta, raw=msg, sequence_number=data['sequenceEnd'])

    async def message_handler(self, msg: str, conn, timestamp: float):
        msg = json.loads(msg, parse_float=Decimal)

        if 'topic' not in msg:
            if msg['type'] == 'error':
                LOG.warning("%s: error from exchange %s", self.id, msg)
                return
            elif msg['type'] in {'welcome', 'ack'}:
                return
            else:
                LOG.warning("%s: Unhandled message type %s", self.id, msg)
                return

        if ":" in msg['topic']:
            topic, symbol = msg['topic'].split(":", 1)
        else:
            topic = msg['topic']

        topic = self.exchange_channel_to_std(topic)

        if topic == TICKER:
            await self._ticker(msg, symbol, timestamp)
        elif topic == TRADES:
            await self._trades(msg, symbol, timestamp)
        elif topic == CANDLES:
            await self._candles(msg, symbol, timestamp)
        elif topic == L2_BOOK:
            await self._process_l2_book(msg, symbol, timestamp)
        elif topic == ORDER_INFO:
            await self._order_update(msg, timestamp)
        elif topic == BALANCES:
            await self._balance(msg, timestamp)
        else:
            LOG.warning("%s: Unhandled message type %s", self.id, msg)

    async def subscribe(self, conn: AsyncConnection):
        self.__reset()
        for chan in self.subscription:
            symbols = list(self.subscription[chan])
            nchan = self.exchange_channel_to_std(chan)
            if nchan == CANDLES:
                for symbol in symbols:
                    await conn.write(json.dumps({
                        'id': 1,
                        'type': 'subscribe',
                        'topic': f"{chan}:{symbol}_{self.candle_interval_map[self.candle_interval]}",
                        'privateChannel': False,
                        'response': True
                    }))
            elif nchan in (BALANCES, ORDER_INFO):
                await conn.write(json.dumps({
                    'id': 1,
                    'type': 'subscribe',
                    'topic': chan,
                    'privateChannel': True,
                    'response': True
                }))
                if nchan == BALANCES:
                    await self._balance_snapshot()
            else:
                for slice_index in range(0, len(symbols), 100):
                    await conn.write(json.dumps({
                        'id': 1,
                        'type': 'subscribe',
                        'topic': f"{chan}:{','.join(symbols[slice_index: slice_index+100])}",
                        'privateChannel': False,
                        'response': True
                    }))
