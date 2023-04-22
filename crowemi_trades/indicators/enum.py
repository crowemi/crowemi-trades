from enum import Enum

from crowemi_trades.indicators.session_indicator import SessionIndicator


INDICATORS = {
    # TODO: generate this using reflection for all classes in crowemi_trades.indicators
    SessionIndicator.__name__: SessionIndicator,
}
