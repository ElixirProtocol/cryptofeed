import logging
import time
from random import randint
from decimal import Decimal
from typing import Dict, Tuple
import asyncio

from yapic import json

from cryptofeed.connection import AsyncConnection, RestEndpoint, WebsocketEndpoint
from cryptofeed.defines import BID, ASK, BUY, INJECTIVE, L2_BOOK, TRADES, ORDER_INFO, BALANCES, SELL
from cryptofeed.feed import Feed
from cryptofeed.symbols import Symbol, Symbols
from cryptofeed.types import Trade, OrderInfo, Balance
from cryptofeed.exchanges.mixins.injective_rest import InjectiveRestMixin



LOG = logging.getLogger('feedhandler')


class Injective(Feed, InjectiveRestMixin):
    id = INJECTIVE
    websocket_endpoints = []
    rest_endpoints = [RestEndpoint('https://k8s.mainnet.exchange.grpc-web.injective.network:443')]

    rest_channels = (
        L2_BOOK,
        TRADES,
        ORDER_INFO,
        BALANCES,
    )
    websocket_channels = {
        L2_BOOK: '/api/exchange/spot/v1/ordersHistory',
        TRADES: '/api/exchange/spot/v1/ordersHistory',
        ORDER_INFO: '/api/exchange/spot/v1/ordersHistory',
        BALANCES: '/api/exchange/spot/v1/ordersHistory',
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
        LOG.error("mapping")
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
        LOG.error("parse")
        info = {'instrument_type': {}}
        ret = {}
        mapping = {"INJ-USDT": 0, "WBTC-USDC": 1, "WETH-USDC": 3}

        for k, v in mapping.items():
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
        LOG.error("book")
        product_id = msg['product_id']
        pair = self.exchange_symbol_to_std_symbol(product_id)
        delta = {BID: [], ASK: []}

        if pair not in self._l2_book:
            self._l2_book[pair] = await self.l2_book(pair)

        if msg['type'] == 'book_depth':
            updated = False

            for side, key in ((BID, 'bids'), (ASK, 'asks')):
                for data in msg[key]:
                    price = Decimal(data[0])
                    amount = Decimal(data[1])

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
        LOG.error("trade")
        pair = self.exchange_symbol_to_std_symbol(msg['product_id'])

        trade_side = BUY if msg['is_taker_buyer'] else SELL
        trade_size = Decimal(msg['taker_qty'])
        trade_price = Decimal(msg['price'])
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
        LOG.error("orderupd")
        oi = OrderInfo(
            self.id,
            self.exchange_symbol_to_std_symbol(msg['product_id']),
            msg['order_digest'],
            BUY if msg['is_bid'] else SELL,
            "FILLED",
            None,
            Decimal(msg['price']),
            Decimal(msg['original_qty']),
            Decimal(msg['remaining_qty']),
            self.timestamp_normalize(msg['timestamp']),
            msg['subaccount'],
            msg['order_digest'],
            raw=msg
        )
        await self.callback(ORDER_INFO, oi, timestamp)

    async def _balance(self, msg: dict, timestamp: float):
        LOG.error("balance")
        b = Balance(
            self.id,
            self.exchange_symbol_to_std_symbol(msg['product_id']),
            Decimal(msg['amount']),
            Decimal(0),
            self.timestamp_normalize(msg['timestamp']),
            raw=msg)
        await self.callback(BALANCES, b, timestamp)

    async def message_handler(self, msg: str, conn: AsyncConnection, timestamp: float):
        LOG.error("msg")
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
        elif 'result' in msg and msg['result'] is None:
            return
        else:
            LOG.warning("%s: Invalid message type %s", self.id, msg)
