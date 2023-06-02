import os
from rocketmq import foo, logger


def test_passing():
    assert (1, 2, 3) == (1, 2, 3)
    logger.info("foo.bar=%d", foo.bar)
    logger.info("test_passing")
