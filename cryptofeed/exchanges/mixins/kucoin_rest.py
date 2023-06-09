import base64
from decimal import Decimal
import hmac
import logging
import time
from urllib.parse import urlencode

from yapic import json

from cryptofeed.exchange import RestExchange
from cryptofeed.defines import BUY, SELL, TRADE_HISTORY, GET, POST, DELETE

LOG = logging.getLogger('feedhandler')


class KucoinRestMixin(RestExchange):
    api = "https://api.kucoin.com"
    sandbox_api = "https://openapi-sandbox.kucoin.com"
    rest_channels = (
        TRADE_HISTORY
    )

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
    
    async def _request(self, method: str, endpoint: str, auth: bool = False, payload={}, api=None):
        query_string = urlencode(payload)

        if not api:
            api = self.api

        if self.sandbox:
            api = self.sandbox_api

        url = f'{api}{endpoint}?{query_string}'
        if auth:
            str_to_sign = method + endpoint + "?" + query_string
            header = self.generate_token(str_to_sign)
        if method == GET:
            data = await self.http_conn.read(url, header=header)
        elif method == POST:
            data = await self.http_conn.write(url, msg=None, header=header)
        elif method == DELETE:
            data = await self.http_conn.delete(url, header=header)
        return json.loads(data, parse_float=Decimal)
    
    async def trade_history(self, symbol: str, start=None, end=None):
        sym = self.std_symbol_to_exchange_symbol(symbol)
        params = { 'symbol': sym }

        if start:
            params['startAt'] = self._datetime_normalize(start) * 1000

        data = await self._request(GET, '/api/v1/fills', auth=True, payload=params)
        return [
            {
                'symbol': symbol,
                'price': Decimal(trade['price']),
                'amount': Decimal(trade['size']),
                'timestamp': trade['createdAt'] / 1000,
                'side': BUY if trade['side'].lower() == 'buy' else SELL,
                'fee_currency': trade['feeCurrency'],
                'fee_amount': trade['fee'],
                'trade_id': trade['tradeId'],
                'order_id': trade['orderId']
            }
            for trade in data["data"]["items"]
        ]
