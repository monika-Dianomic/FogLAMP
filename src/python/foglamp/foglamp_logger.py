import logging
from foglamp.postgres_handler import AsyncPostgresHandler
from foglamp.configurator import FOGLAMP_DIR

"""
Set custom log levels that may come from Coap sensor. This is being done to record logs more
meaningfully. Also, the custom log levels may be picked from a pre defined db table.
"""

# Custom Sensor log levels
DEBUG_SENSOR1 = 51
DEBUG_SENSOR2 = 52

logging.addLevelName(DEBUG_SENSOR1, "SENSOR1")
logging.addLevelName(DEBUG_SENSOR2, "SENSOR2")

def sensor1(self, message, *args, **kws):
    # Yes, logger takes its '*args' as 'args'.
    if self.isEnabledFor(DEBUG_SENSOR1):
        self._log(DEBUG_SENSOR1, message, args, **kws)

def sensor2(self, message, *args, **kws):
    # Yes, logger takes its '*args' as 'args'.
    if self.isEnabledFor(DEBUG_SENSOR2):
        self._log(DEBUG_SENSOR2, message, args, **kws)

logging.Logger.sensor1 = sensor1
logging.Logger.sensor2 = sensor2

foglamp_logger = logging.getLogger(__name__)
foglamp_logger.setLevel(logging.DEBUG)

foglamp_log_formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(name)s: %(message)s [%(filename)s/%(lineno)d %(module)s/%(funcName)s]')

foglamp_filehandler = logging.FileHandler(FOGLAMP_DIR+'/fog.log')
foglamp_pghandler = AsyncPostgresHandler()

foglamp_filehandler.setFormatter(foglamp_log_formatter)
foglamp_pghandler.setFormatter(foglamp_log_formatter)

foglamp_logger.addHandler(foglamp_filehandler)
foglamp_logger.addHandler(foglamp_pghandler)
