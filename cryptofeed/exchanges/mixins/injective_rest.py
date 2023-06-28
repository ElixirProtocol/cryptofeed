from decimal import Decimal
import time
import logging
import asyncio

from yapic import json

from cryptofeed.exchange import RestExchange
from cryptofeed.defines import L2_BOOK, BUY, SELL
from cryptofeed.types import OrderBook, Balance

from pyinjective.composer import Composer as ProtoMsgComposer
from pyinjective.async_client import AsyncClient
from pyinjective.transaction import Transaction
from pyinjective.constant import Network
from pyinjective.wallet import PrivateKey

LOG = logging.getLogger('feedhandler')

# TODO - The fetch_markets function already cover these
markets = {
    'WETH/USDC': '0x01e920e081b6f3b2e5183399d5b6733bb6f80319e6be3805b95cb7236910ff0e',
    'INJ/USDC': '0xe0dc13205fb8b23111d8555a6402681965223135d368eeeb964681f9ff12eb2a',
    'USDT/USDC': '0x8b1a4d3e8f6b559e30e40922ee3662dd78edf7042330d4d620d188699d1a9715',
    'LINK/USDC': '0xfe93c19c0a072c8dd208b96694e024305a7dff01bbf12cac2bfa81b246c69040',
    'AAVE/USDC': '0xcdfbfaf1f24055e89b3c7cc763b8cb46ffff08cdc38c999d01f58d64af75dca9',
    'MATIC/USDC': '0x5abfffe9079d53e0bf8ee9b3064b427acc3d71d6ba58a44235abe38f60115678',
    'UNI/USDT': '0xe8bf0467208c24209c1cf0fd64833fa43eb6e8035869f9d043dbff815ab76d01',
    'SUSHI/USDC': '0x9a629b947b6f946af4f6076cfda67f3535d73ee3cef6176cf6d9c8d6b0a03f37',
    'GRT/USDC': '0xa43d2be9861efb0d188b136cef0ae2150f80e08ec318392df654520dd359fcd7',
    'INJ/USDT': '0xa508cb32923323679f29a032c70342c147c17d0145625922b0ef22e955c844c0',
    'MATIC/USDT': '0x28f3c9897e23750bf653889224f93390c467b83c86d736af79431958fff833d1',
    'UNI/USDC': '0x09cc2c28fbedbdd677e07924653f8f583d0ee5886e74046e7f114210d990784b',
    'LINK/USDT': '0x26413a70c9b78a495023e5ab8003c9cf963ef963f6755f8b57255feb5744bf31',
    'WETH/USDT': '0xd1956e20d74eeb1febe31cd37060781ff1cb266f49e0512b446a5fafa9a16034',
    'AAVE/USDT': '0x01edfab47f124748dc89998eb33144af734484ba07099014594321729a0ca16b',
    'GRT/USDT': '0x29255e99290ff967bc8b351ce5b1cb08bc76a9a9d012133fb242bdf92cd28d89',
    'SUSHI/USDT': '0x0c9f98c99b23e89dbf6a60bec05372790b39e03da0f86dd0208fc8e28751bd8c',
    'SNX/USDT': '0x51092ddec80dfd0d41fee1a7d93c8465de47cd33966c8af8ee66c14fe341a545',
    'QNT/USDT': '0xbe9d4a0a768c7e8efb6740be76af955928f93c247e0b3a1a106184c6cf3216a7',
    'WBTC/USDC': '0x170a06eb653548f67e94b0fcb82c5258c83b0a2b62ed24c55749d5ac77bc7621',
    'AXS/USDT': '0x7471d361b90fc8541267bd088f498c2a461a2c0c57ff2b9a08279480e803b470',
    'ATOM/USDT': '0x0511ddc4e6586f3bfe1acb2dd905f8b8a82c97e1edaef654b12ca7e6031ca0fa',
    'GF/USDT': '0x7f71c4fba375c964be8db7fc7a5275d974f8c6cdc4d758f2ac4997f106bb052b',
    'UST/USDT': '0x0f1a11df46d748c2b20681273d9528021522c6a0db00de4684503bbd53bef16e',
    'LUNA/UST': '0xdce84d5e9c4560b549256f34583fb4ed07c82026987451d5da361e6e238287b3',
    'INJ/UST': '0xfbc729e93b05b4c48916c1433c9f9c2ddb24605a73483303ea0f87a8886b52af',
    'HUAHUA/USDT': '0xf04d1b7acf40b331d239fcff7950f98a4f2ab7adb2ceb8f65aa32ac29455d7b4',
    'APE/USDT': '0x572f05fd93a6c2c4611b2eba1a0a36e102b6a592781956f0128a27662d84f112',
    'EVMOS/USDT': '0x719f1617efc6e998472b70436549e0999fab8c05701177b15ba8910f2c5e7ab2',
    'XPRT/USDT': '0x4d030dccc9564ab1536a7751c3a566fa3adcb6a08ac807edc82890f2a6ec4fed',
    'DOT/USDT': '0xd5f5895102b67300a2f8f2c2e4b8d7c4c820d612bc93c039ba8cb5b93ccedf22',
    'SOL/USDC': '0x84ba79ffde31db8273a9655eb515cb6cadfdf451b8f57b83eb3f78dca5bbbe6d',
    'USDCso/USDCet': '0xb825e2e4dbe369446e454e21c16e041cbc4d95d73f025c369f92210e82d2106f',
    'USDC/USDCet': '0xf66f797a0ff49bd2170a04d288ca3f13b5df1c822a7b0cc4204aca64a5860666',
    'STRD/USDT': '0xcd4b823ad32db2245b61bf498936145d22cdedab808d2f9d65100330da315d29',
    'SOMM/USDT': '0x0686357b934c761784d58a2b8b12618dfe557de108a220e06f8f6580abb83aab',
    'CHZ/USDC': '0x4fa0bd2c2adbfe077f58395c18a72f5cbf89532743e3bddf43bc7aba706b0b74',
    'CANTO/USDT': '0xa7fb70ac87e220f3ea7f7f77faf48b47b3575a9f7ad22291f04a02799e631ca9',
    'CRE/USDT': '0xe03df6e1571acb076c3d8f22564a692413b6843ad2df67411d8d8e56449c7ff4',
    'LDO/USDC': '0x66a113e1f0c57196985f8f1f1cfce2f220fa0a96bca39360c70b6788a0bc06e0',
    'ARB/USDT': '0x1bba49ea1eb64958a19b66c450e241f17151bc2e5ea81ed5e2793af45598b906',
    'WMATIC/USDC': '0xba33c2cdb84b9ad941f5b76c74e2710cf35f6479730903e93715f73f2f5d44be',
    'WMATIC/USDT': '0xb9a07515a5c239fcbfa3e25eaa829a03d46c4b52b9ab8ee6be471e9eb0e9ea31'
}

test_markets = {
    'INJ/USDT': '0x0611780ba69656949525013d947713300f56c37b6175e02f26bffa495c3208fe',
    'APE/USDT': '0x7a57e705bb4e09c88aecfc295569481dbf2fe1d5efe364651fbe72385938e9b0',
    'INJ/APE': '0xabed4a28baf4617bd4e04e4d71157c45ff6f95f181dee557aae59b4d1009aa97',
    'WETH/USDT': '0xa97182f11f1aa5339c7f4c3fe3cc1c69b39079f11b864c86d912956c5c2db75c',
    'WBTC/USDT': '0x1c315bd2cfcc769a8d8eca49ce7b1bc5fb0353bfcb9fa82895fe0c1c2a62306e',
    'ATOM/USDT': '0x491ee4fae7956dd72b6a97805046ffef65892e1d3254c559c18056a519b2ca15',
}


class InjectiveMarket:
    def __init__(
        self,
        symbol,
        status,
        market_id,
        base_denom,
        quote_denom,
        maker_fee_rate,
        taker_fee_rate,
        servicer_provider_fee,
        min_price_tick_size,
        min_quantity_tick_size
    ):
        self.symbol = symbol
        self.status = status
        self.market_id = market_id
        self.base_denom = base_denom
        self.quote_denom = quote_denom
        self.maker_fee_rate = maker_fee_rate
        self.taker_fee_rate = taker_fee_rate
        self.servicer_provider_fee = servicer_provider_fee
        self.min_price_tick_size = min_price_tick_size
        self.min_quantity_tick_size = min_quantity_tick_size


class InjectiveRestMixin(RestExchange):
    api = "https://k8s.mainnet.exchange.grpc-web.injective.network:443"
    sandbox_api = "https://k8s.testnet.explorer.grpc-web.injective.network"
    rest_channels = (
        L2_BOOK
    )
    markets = {}
    denoms = {}

    async def balance_snapshot(self):
        data = await self.http_conn.read(f"{self.api}/query?type=subaccount_info&subaccount={self.account_name}")
        data = json.loads(data, parse_float=Decimal)
        result = []
        for balance in data['data']['spot_balances']:
            b = Balance(
                self.id,
                self.exchange_symbol_to_std_symbol(balance['product_id']).split("-")[0],
                Decimal(balance['balance']['amount']),
                Decimal(0),  # Consider '0' as we don't have a corresponding value in the JSON data.
                timestamp=int(time.time()),
                raw=balance)
            result.append(b)
        return result

    async def l2_book(self, symbol: str, retry_count=1, retry_delay=60) -> OrderBook:
        ret = OrderBook(self.id, symbol)
        # sym = self.std_symbol_to_exchange_symbol(symbol)
        if symbol in self.markets:
            market_id = self.markets[symbol].market_id
            base_endpoint = "/api/exchange/spot/v2/orderbooks"
            query = f"?subaccountId={self.subaccount}&marketId={market_id}"
            data = await self.http_conn.read(f"{self.api}{base_endpoint}{query}")
            data = json.loads(data, parse_float=Decimal)
            if len(data["order_books"]) > 0:
                order_book = data["order_books"][0]["order_book"]
                ret.book.bids = {Decimal(entry["price"]): Decimal(entry["quantity"]) for entry in order_book['buys']}
                ret.book.asks = {Decimal(entry["price"]): Decimal(entry["quantity"]) for entry in order_book['sells']}
                ret.timestamp = data['timestamp']
            return ret
        else:
            LOG.error(f"Symbol {symbol} not found")
            return None

    async def trade_history(self, symbol: str, start=None, end=None):
        # sym = self.std_symbol_to_exchange_symbol(symbol)

        if symbol in self.markets:
            market_id = self.markets[symbol].market_id
            base_endpoint = "/api/exchange/spot/v1/ordersHistory"
            query = f"?subaccountId={self.subaccount}&marketId={market_id}"
            data = await self.http_conn.read(f"{self.api}{base_endpoint}{query}")
            data = json.loads(data, parse_float=Decimal)

            return [
                {
                    'symbol': symbol,
                    'price': Decimal(order['price']),
                    'amount': Decimal(order['quantity']),
                    'timestamp': order['createdAt'],
                    'side': BUY if order['direction'] == 'buy' else SELL,
                    'trade_id': order['txHash'],
                    'order_id': order['orderHash']
                }
                for order in data['orders']
            ]
        else:
            LOG.error(f"Symbol {symbol} not found")
            return None

    async def fetch_markets(self):
        base_endpoint = "/api/exchange/spot/v1/markets"
        data = await self.http_conn.read(f"{self.api}{base_endpoint}")
        markets = json.loads(data, parse_float=Decimal)
        for market in markets["markets"]:
            symbol = market["ticker"].replace("/", "-").lower()
            inj_market = InjectiveMarket(
                symbol=symbol,
                status = market["marketStatus"],
                market_id = market["marketId"],
                base_denom = market["baseDenom"],
                quote_denom = market["quoteDenom"],
                maker_fee_rate = market["makerFeeRate"],
                taker_fee_rate = market["takerFeeRate"],
                servicer_provider_fee = market["serviceProviderFee"],
                min_price_tick_size = market["minPriceTickSize"],
                min_quantity_tick_size = market["minQuantityTickSize"],
            )
            self.markets[symbol] = inj_market
            base, quote = symbol.split('-')
            if base not in self.denoms:
                self.denoms[base] = market["baseDenom"]
            if quote not in self.denoms:
                self.denoms[quote] = market["quoteDenom"]
