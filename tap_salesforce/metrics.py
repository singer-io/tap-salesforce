from typing import Optional, Any
from datetime import datetime, timedelta
import logging


class Metrics:
    sample_rate_seconds: int
    format: str
    _value: Any
    _last_sample: Optional[datetime] = None

    def __init__(
        self,
        format: str,
        sample_rate_seconds: int = 15,
        logger: Optional[logging.Logger] = None,
    ):
        if format == "" or None:
            raise ValueError(f"format string cannot be empty string")

        self.format = format

        if sample_rate_seconds < 0:
            raise ValueError(
                f"invalid sample_rate: sample rate must be greater than 0: {sample_rate_seconds}"
            )

        self.sample_rate_seconds = sample_rate_seconds

        if logger is None:
            self._logger = logging.getLogger()
        else:
            self._logger = logger

    def gauge(self, value: Any):
        self.value = value

        now = datetime.utcnow()

        if self._last_sample is None:
            self._last_sample = now
            return

        time_since_last_sample = now - self._last_sample
        if time_since_last_sample < timedelta(seconds=self.sample_rate_seconds):
            return

        self._logger.info(self.format, self.value)
