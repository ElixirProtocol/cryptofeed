from decimal import Decimal
import hashlib
import hmac
import logging
import time
from urllib.parse import urlencode

from yapic import json

from cryptofeed.exchange import RestExchange
from cryptofeed.defines import BUY, SELL, TRADE_HISTORY, GET, POST, DELETE

LOG = logging.getLogger('feedhandler')


class GateioRestMixin(RestExchange):
    api = "https://api.gateio.ws"
    rest_channels = (
        TRADE_HISTORY
    )

    def gen_sign_http(self, method, url, query_string=None, payload_string=None):
        t = time.time()
        m = hashlib.sha512()
        m.update((payload_string or "").encode('utf-8'))
        hashed_payload = m.hexdigest()
        s = '%s\n%s\n%s\n%s\n%s' % (method, url, query_string or "", hashed_payload, t)
        sign = hmac.new(self.key_secret.encode('utf-8'), s.encode('utf-8'), hashlib.sha512).hexdigest()
        return {'method': 'api_key', 'KEY': self.key_id, 'Timestamp': str(t), 'SIGN': sign}

    async def _request(self, method: str, endpoint: str, auth: bool = False, payload={}, api=None):
        header = {'Accept': 'application/json', 'Content-Type': 'application/json'}
        query_string = urlencode(payload)

        if not api:
            api = self.api

        url = f'{api}{endpoint}?{query_string}'
        if auth:
            sign_headers = self.gen_sign_http(method, endpoint, query_string)
            header.update(sign_headers)
        if method == GET:
            data = await self.http_conn.read(url, header=header)
        elif method == POST:
            data = await self.http_conn.write(url, msg=None, header=header)
        elif method == DELETE:
            data = await self.http_conn.delete(url, header=header)
        return json.loads(data, parse_float=Decimal)
    
    async def trade_history(self, symbol: str, start=None, end=None):
        sym = self.std_symbol_to_exchange_symbol(symbol)
        params = { 'currency_pair': sym }

        if start:
            params['from'] = self._datetime_normalize(start) * 1000

        data = await self._request(GET, '/api/v4/spot/my_trades', auth=True, payload=params)
        return [
            {
                'symbol': symbol,
                'price': Decimal(trade['price']),
                'amount': Decimal(trade['amount']),
                'timestamp': trade['create_time'] / 1000,
                'side': BUY if trade['side'].lower() == 'buy' else SELL,
                'fee_currency': trade['fee_currency'],
                'fee_amount': trade['fee'],
                'trade_id': trade['id'],
                'order_id': trade['order_id']
            }
            for trade in data
        ]
