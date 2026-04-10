import logging
from pcp_arbitrage.models import FeeRates
from pcp_arbitrage.okx_client import OKXRestClient

logger = logging.getLogger(__name__)


async def fetch_fee_rates(client: OKXRestClient) -> FeeRates:
    """
    Fetch option and future fee rates from OKX.

    OKX returns taker rates as negative strings (e.g. "-0.0002").
    We take abs() so callers work with positive rates.

    Note: for OPTION instType the OKX endpoint requires a uly (underlying) to
    return meaningful per-contract rates. We use BTC-USD as the canonical uly;
    OKX fee tiers are account-level and the same across underlyings.
    """
    option_data = await client.get_fee_rates("OPTION", uly="BTC-USD")
    future_data = await client.get_fee_rates("FUTURES")

    # taker rate fields: "taker" for options, "taker" for futures
    option_taker_rate = abs(float(option_data.get("taker", "0.0002")))
    option_maker_rate = abs(float(option_data.get("maker", "0.0001")))
    future_taker_rate = abs(float(future_data.get("taker", "0.0005")))
    future_maker_rate = abs(float(future_data.get("maker", "0.0002")))

    logger.info(
        "[fee_fetcher] option taker=%.6f maker=%.6f  future taker=%.6f maker=%.6f",
        option_taker_rate, option_maker_rate, future_taker_rate, future_maker_rate,
    )
    return FeeRates(
        option_taker_rate=option_taker_rate,
        option_maker_rate=option_maker_rate,
        future_taker_rate=future_taker_rate,
        future_maker_rate=future_maker_rate,
    )
