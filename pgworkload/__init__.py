import logging

logger = logging.getLogger(__package__)
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s [%(levelname)s] (%(processName)s %(process)d %(threadName)s) %(name)s:%(funcName)s:%(lineno)d - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)
