import math
from datetime import timedelta

from google.protobuf.duration_pb2 import Duration


class ExponentialBackoffRetryPolicy:
    """A class implementing exponential backoff retry policy."""

    def __init__(self, max_attempts, initial_backoff, max_backoff, backoff_multiplier):
        """Initialize an ExponentialBackoffRetryPolicy instance.

        :param max_attempts: Maximum number of retry attempts.
        :param initial_backoff: Initial delay duration before the first retry.
        :param max_backoff: Maximum delay duration between retries.
        :param backoff_multiplier: Multiplier that determines the delay factor between retries.
        """
        self._max_attempts = max_attempts
        self.initial_backoff = initial_backoff
        self.max_backoff = max_backoff
        self.backoff_multiplier = backoff_multiplier

    def get_max_attempts(self):
        """Get maximum number of retry attempts.

        :return: Maximum number of retry attempts.
        """
        return self._max_attempts

    def inherit_backoff(self, retry_policy):
        """Inherit backoff parameters from another retry policy.

        :param retry_policy: The retry policy to inherit from.
        :return: An instance of ExponentialBackoffRetryPolicy with inherited parameters.
        :raise ValueError: If the strategy of the retry policy is not ExponentialBackoff.
        """
        if retry_policy.strategy_case != "ExponentialBackoff":
            raise ValueError("Strategy must be exponential backoff")
        return self._inherit_backoff(retry_policy.exponential_backoff)

    def _inherit_backoff(self, retry_policy):
        """Inherit backoff parameters from another retry policy.

        :param retry_policy: The retry policy to inherit from.
        :return: An instance of ExponentialBackoffRetryPolicy with inherited parameters.
        """
        return ExponentialBackoffRetryPolicy(self._max_attempts,
                                             retry_policy.initial.ToTimedelta(),
                                             retry_policy.max.ToTimedelta(),
                                             retry_policy.multiplier)

    def get_next_attempt_delay(self, attempt):
        """Calculate the delay before the next retry attempt.

        :param attempt: The number of the current attempt.
        :return: The delay before the next attempt.
        """
        delay_seconds = min(
            self.initial_backoff.total_seconds() * math.pow(self.backoff_multiplier, 1.0 * (attempt - 1)),
            self.max_backoff.total_seconds())
        return timedelta(seconds=delay_seconds) if delay_seconds >= 0 else timedelta(seconds=0)

    @staticmethod
    def immediately_retry_policy(max_attempts):
        """Create a retry policy that makes immediate retries.

        :param max_attempts: Maximum number of retry attempts.
        :return: An instance of ExponentialBackoffRetryPolicy with no delay between retries.
        """
        return ExponentialBackoffRetryPolicy(max_attempts, timedelta(seconds=0), timedelta(seconds=0), 1)

    def to_protobuf(self):
        """Convert the ExponentialBackoffRetryPolicy instance to protobuf.

        :return: A protobuf message that represents the ExponentialBackoffRetryPolicy instance.
        """
        exponential_backoff = {
            'Multiplier': self.backoff_multiplier,
            'Max': Duration.FromTimedelta(self.max_backoff),
            'Initial': Duration.FromTimedelta(self.initial_backoff)
        }
        return {
            'MaxAttempts': self._max_attempts,
            'ExponentialBackoff': exponential_backoff
        }
