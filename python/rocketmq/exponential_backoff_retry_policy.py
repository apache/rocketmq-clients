from datetime import timedelta
import math
from google.protobuf.duration_pb2 import Duration
from protocol.definition_pb2 import ExponentialBackoff as ProtoExponentialBackoff


class ExponentialBackoffRetryPolicy:
    def __init__(self, max_attempts, initial_backoff, max_backoff, backoff_multiplier):
        self._max_attempts = max_attempts
        self.initial_backoff = initial_backoff
        self.max_backoff = max_backoff
        self.backoff_multiplier = backoff_multiplier

    def get_max_attempts(self):
        return self._max_attempts

    def inherit_backoff(self, retry_policy):
        if retry_policy.strategy_case != "ExponentialBackoff":
            raise ValueError("Strategy must be exponential backoff")
        return self._inherit_backoff(retry_policy.exponential_backoff)

    def _inherit_backoff(self, retry_policy):
        return ExponentialBackoffRetryPolicy(self._max_attempts, 
                                             retry_policy.initial.ToTimedelta(),
                                             retry_policy.max.ToTimedelta(), 
                                             retry_policy.multiplier)

    def get_next_attempt_delay(self, attempt):
        delay_seconds = min(
            self.initial_backoff.total_seconds() * math.pow(self.backoff_multiplier, 1.0 * (attempt - 1)),
            self.max_backoff.total_seconds())
        return timedelta(seconds=delay_seconds) if delay_seconds >= 0 else timedelta(seconds=0)

    @staticmethod
    def immediately_retry_policy(max_attempts):
        return ExponentialBackoffRetryPolicy(max_attempts, timedelta(seconds=0), timedelta(seconds=0), 1)

    def to_protobuf(self):
        exponential_backoff = {
            'Multiplier': self.backoff_multiplier,
            'Max': Duration.FromTimedelta(self.max_backoff),
            'Initial': Duration.FromTimedelta(self.initial_backoff)
        }
        return {
            'MaxAttempts': self._max_attempts,
            'ExponentialBackoff': exponential_backoff
        }
