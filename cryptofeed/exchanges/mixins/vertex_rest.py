from decimal import Decimal
import time
import logging

from yapic import json

from cryptofeed.exchange import RestExchange
from cryptofeed.defines import L2_BOOK, BUY, SELL
from cryptofeed.types import OrderBook, Balance

LOG = logging.getLogger('feedhandler')

UNITS = Decimal(1e18)


class VertexRestMixin(RestExchange):
    api = "https://prod.vertexprotocol-backend.com"
    sandbox_api = "https://test.vertexprotocol-backend.com"
    rest_channels = (
        L2_BOOK
    )

    def __init__(self, sandbox=False, **kwargs):
        self.sandbox = sandbox

    def get_api(self):
        if self.sandbox:
            return self.sandbox_api
        else:
            return self.api

    async def balance_snapshot(self):
        data = await self.http_conn.read(f"{self.get_api()}/query?type=subaccount_info&subaccount={self.account_name}")
        data = json.loads(data, parse_float=Decimal)
        result = []

        for balance in data['data']['spot_balances']:
            b = Balance(
                self.id,
                self.exchange_symbol_to_std_symbol(balance['product_id']).split("-")[0],
                Decimal(balance['balance']['amount']) / UNITS,
                Decimal(0),  # Consider '0' as we don't have a corresponding value in the JSON data.
                timestamp=int(time.time()),
                raw=balance)
            result.append(b)
        return result

    async def l2_book(self, symbol: str, retry_count=1, retry_delay=60) -> OrderBook:
        ret = OrderBook(self.id, symbol)
        sym = self.std_symbol_to_exchange_symbol(symbol)
        data = await self.http_conn.read(f"{self.get_api()}/query?type=market_liquidity&product_id={sym}&depth={self.max_depth}", retry_count=retry_count, retry_delay=retry_delay)
        data = json.loads(data, parse_float=Decimal)
        ret.book.bids = {Decimal(entry[0]) / UNITS: Decimal(entry[1]) / UNITS for entry in data['data']['bids']}
        ret.book.asks = {Decimal(entry[0]) / UNITS: Decimal(entry[1]) / UNITS for entry in data['data']['asks']}
        ret.timestamp = self.timestamp_normalize(data['data']['timestamp'])
        return ret

    async def trade_history(self, symbol: str, start=None, end=None):
        sym = self.std_symbol_to_exchange_symbol(symbol)

        data = await self.http_conn.read(f"{self.get_api()}/query?type=subaccount_orders&product_id={sym}&sender={self.account_name}")
        data = json.loads(data, parse_float=Decimal)

        return [
            {
                'symbol': symbol,
                'price': Decimal(order['price_x18']),
                'amount': Decimal(order['amount']),
                'timestamp': order['placed_at'],
                'side': BUY if Decimal(order['amount']) > 0 else SELL,
                'trade_id': order['digest'],
                'order_id': order['digest']
            }
            for order in data['data']['orders']
        ]

