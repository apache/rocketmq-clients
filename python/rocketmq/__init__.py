import os
import logging

logger = logging.getLogger("rocketmqlogger")
logger.setLevel(logging.DEBUG)

log_path = os.path.join(
    os.path.expanduser("~"), "logs", "rocketmq", "rocketmq-client.log"
)
file_handler = logging.FileHandler(log_path)
file_handler.setLevel(logging.DEBUG)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)

formatter = logging.Formatter(
    "%(asctime)s [%(levelname)s] [%(process)d] [%(filename)s#%(funcName)s:%(lineno)d] %(message)s"
)
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)
