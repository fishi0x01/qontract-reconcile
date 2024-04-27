from collections.abc import Callable, Mapping
from typing import Any

from reconcile.saas_auto_promotions_manager.subscriber import Subscriber


def test_encoding(
    subscriber_builder: Callable[[Mapping[str, Any]], Subscriber],
) -> None:
    subscribers = [subscriber_builder({})]
    data = Subscriber.to_mr_data(subscribers)
    decoded_subscribers = Subscriber.from_mr_data(data)

    assert decoded_subscribers == subscribers
