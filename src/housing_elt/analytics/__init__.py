"""Analytics-ready housing fact construction and output."""

from housing_elt.analytics.aggregation import build_analytics_fact
from housing_elt.analytics.writer import write_analytics_fact

__all__ = ["build_analytics_fact", "write_analytics_fact"]
