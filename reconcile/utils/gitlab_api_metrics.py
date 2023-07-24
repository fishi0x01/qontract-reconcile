from collections.abc import Callable
from typing import Any

from pydantic import BaseModel

from reconcile.utils import metrics
from reconcile.utils.metrics import (
    CounterMetric,
)

class GitlabAPIBaseMetric(BaseModel):
    "Base class for Gitlab API metrics"
    integration: str


class GitlabAPI5xxCounter(
    GitlabAPIBaseMetric, CounterMetric
):
    "Counter for 5xx from Gitlab API"
    kind: str

    @classmethod
    def name(cls) -> str:
        return "gitlab_api_5xx"


def count_api_errors(integration: str) -> Callable:
    def func_wrapper(function: Callable) -> Callable:
        def wrapper(*args: Any, **kwargs: Any):
            try:
                return function(*args, **kwargs)
            except:
                metrics.inc_counter(
                    GitlabAPI5xxCounter(
                        integration=integration,
                        kind=""
                    )
                )
            return None
        return wrapper
    return func_wrapper
