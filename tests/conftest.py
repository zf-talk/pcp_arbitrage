import pytest


@pytest.fixture
def btc_lot_size() -> float:
    return 0.01


@pytest.fixture
def eth_lot_size() -> float:
    return 0.1
