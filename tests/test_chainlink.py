from app.data.chainlink import _extract_price


def test_extract_chainlink_price_from_common_shapes() -> None:
    assert _extract_price({"price": "81234.5"}) == 81234.5
    assert _extract_price({"data": {"answer": 81235}}) == 81235
    assert _extract_price([{"result": {"midPrice": "81236.25"}}]) == 81236.25
