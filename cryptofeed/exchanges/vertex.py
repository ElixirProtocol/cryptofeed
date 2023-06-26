'''
Copyright (C) 2023 Elixir finance

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import logging
import time
from random import randint
from decimal import Decimal
from typing import Dict, Tuple

from yapic import json

from cryptofeed.connection import AsyncConnection, RestEndpoint, WebsocketEndpoint
from cryptofeed.defines import BID, ASK, BUY, VERTEX, L2_BOOK, TRADES, ORDER_INFO, BALANCES, SELL
from cryptofeed.feed import Feed
from cryptofeed.exchanges.mixins.vertex_rest import VertexRestMixin
from cryptofeed.symbols import Symbol, Symbols
from cryptofeed.types import Trade, OrderInfo, Balance

LOG = logging.getLogger('feedhandler')

UNITS = Decimal(1e18)

class Vertex(Feed, VertexRestMixin):
    id = VERTEX
    websocket_endpoints = [WebsocketEndpoint('wss://prod.vertexprotocol-backend.com/subscribe', sandbox="wss://test.vertexprotocol-backend.com/subscribe")]
    rest_endpoints = [RestEndpoint('https://prod.vertexprotocol-backend.com', sandbox="https://test.vertexprotocol-backend.com")]

    websocket_channels = {
        L2_BOOK: 'book_depth',
        TRADES: 'trade',
        ORDER_INFO: 'fill',
        BALANCES: 'position_change',
    }
    request_limit = 10

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.max_depth = 2

    def __reset(self):
        self._l2_book = {}

    @classmethod
    def timestamp_normalize(cls, ts: str) -> float:
        """
        Normalize a timestamp provided as a string in nanoseconds to miliseconds.
        """
        return float(ts) / 1_000_000.0

    @classmethod
    def symbol_mapping(cls, refresh=False) -> Dict:
        if Symbols.populated(cls.id) and not refresh:
            return Symbols.get(cls.id)[0]
        try:
            syms, info = cls._parse_symbol_data()
            Symbols.set(cls.id, syms, info)
            return syms
        except Exception as e:
            LOG.error("%s: Failed to parse symbol information: %s", cls.id, str(e), exc_info=True)
            raise

    @classmethod
    def _parse_symbol_data(cls) -> Tuple[Dict, Dict]:
        info = {'instrument_type': {}}
        ret = {}
        mapping = { "USDC": 0, "WBTC-USDC": 1, "WETH-USDC": 3, "PLACEHOLDER1": 5, "PLACEHOLDER2": 7, "PLACEHOLDER3": 9, "PLACEHOLDER4": 11, "PLACEHOLDER5": 13 }

        for k,v in mapping.items():
            if "-" in k:
                base, quote = k.split("-")
            else:
                base = k
                quote = ''  # Assign an empty string or None, depending on what's appropriate for your use case
            s = Symbol(base, quote)
            ret[s.normalized] = v
            info['instrument_type'][s.normalized] = s.type
        return ret, info

    @classmethod
    def is_authenticated_channel(cls, channel: str) -> bool:
        return False
    
    async def _book(self, msg: dict, timestamp: float):
        product_id = msg['product_id']
        pair = self.exchange_symbol_to_std_symbol(product_id)
        delta = {BID: [], ASK: []}

        if msg['type'] == 'book_depth':
            updated = False

            for side, key in ((BID, 'bids'), (ASK, 'asks')):
                for data in msg[key]:
                    price = Decimal(data[0]) / UNITS
                    amount = Decimal(data[1]) / UNITS

                    updated = True

                    delta[side].append((price, amount))

                    if amount == 0:
                        if price in self._l2_book[pair].book[side]:
                            del self._l2_book[pair].book[side][price]
                    else:
                        self._l2_book[pair].book[side][price] = amount
            if updated:
                await self.book_callback(L2_BOOK, self._l2_book[pair], time.time(), timestamp, delta=delta, raw=msg)

    async def _trade(self, msg: dict, timestamp: float):
        pair = self.exchange_symbol_to_std_symbol(msg['product_id'])
        
        trade_side = BUY if msg['is_taker_buyer'] else SELL
        trade_size = Decimal(msg['taker_qty']) / UNITS
        trade_price = Decimal(msg['price']) / UNITS
        trade_timestamp = self.timestamp_normalize(msg['timestamp'])

        t = Trade(
            self.id,
            pair,
            trade_side,
            trade_size,
            trade_price,
            trade_timestamp,
            raw=msg  # Include the entire message in raw for further reference
        )
        
        await self.callback(TRADES, t, timestamp)

    async def _order_update(self, msg: dict, timestamp: float):
        oi = OrderInfo(
            self.id,
            self.exchange_symbol_to_std_symbol(msg['product_id']),
            msg['order_digest'],
            BUY if msg['is_bid'] else SELL,
            "FILLED",
            None,
            Decimal(msg['price']) / UNITS,
            Decimal(msg['original_qty']) / UNITS,
            Decimal(msg['remaining_qty']),
            self.timestamp_normalize(msg['timestamp']),
            msg['order_digest'],
            msg['subaccount'],
            raw=msg
        )
        await self.callback(ORDER_INFO, oi, timestamp)

    async def _balance(self, msg: dict, timestamp: float):
        b = Balance(
            self.id,
            self.exchange_symbol_to_std_symbol(msg['product_id']),
            Decimal(msg['amount']) / UNITS,
            Decimal(0),
            self.timestamp_normalize(msg['timestamp']),
            raw=msg)
        await self.callback(BALANCES, b, timestamp)

    async def message_handler(self, msg: str, conn: AsyncConnection, timestamp: float):
        msg = json.loads(msg, parse_float=Decimal)

        if 'type' in msg:
            chan = self.exchange_channel_to_std(msg['type'])
            if chan == L2_BOOK:
                await self._book(msg, timestamp)
            elif chan == TRADES:
                await self._trade(msg, timestamp)
            elif chan == ORDER_INFO:
                await self._order_update(msg, timestamp)
            elif chan == BALANCES:
                await self._balance(msg, timestamp)
            else:
                LOG.warning("%s: unexpected channel type received: %s", self.id, msg)
        elif 'result' in msg and msg['result'] == None:
            return
        else:
            LOG.warning("%s: Invalid message type %s", self.id, msg)

    async def subscribe(self, conn: AsyncConnection):
        self.__reset()

        for chan, symbols in self.subscription.items():
            for symbol in symbols:
                if chan == "fill" or chan == "position_change":
                    if chan == "position_change":
                        resp = await self.balance_snapshot()
                        for balance in resp:
                            await self.callback(BALANCES, balance, time.time())
                    msg = {
                        "method": "subscribe",
                        "stream": {
                            "type": chan,
                            "product_id": symbol,
                            "subaccount": self.account_name
                        },
                        "id": randint(1,1000)
                    }
                    await conn.write(json.dumps(msg))
                else:
                    if chan == "book_depth":
                        pair = self.exchange_symbol_to_std_symbol(symbol)
                        book = await self.l2_book(pair)
                        self._l2_book[pair] = book
                        await self.callback(L2_BOOK, book, time.time())
                    msg = {
                        "method": "subscribe",
                        "stream": { "type": chan, "product_id": symbol},
                        "id": randint(1,1000)
                    }
                    await conn.write(json.dumps(msg))
