import datetime
import asyncio
import uuid
import psycopg2
import aiocoap
import aiocoap.resource as resource
import logging
import sqlalchemy as sa
from cbor2 import loads
from sqlalchemy.dialects.postgresql import JSONB
from aiopg.sa import create_engine
import aiopg.sa
from foglamp.configurator import Configurator
from foglamp.coap.postgres_handler import AsyncPostgresHandler

metadata = sa.MetaData()

__tbl__ = sa.Table(
    'sensor_values_t'
    , metadata
    , sa.Column('key', sa.types.VARCHAR(50))
    , sa.Column('data', JSONB))
'''Incoming data is inserted into this table'''

# Custom Sensor log levels
DEBUG_SENSOR1 = 51
DEBUG_SENSOR2 = 52
DEBUG_SENSOR3 = 53
DEBUG_SENSOR4 = 54
DEBUG_SENSOR5 = 55

def set_custom_log_levels():
    logging.addLevelName(DEBUG_SENSOR1, "SENSOR1")
    logging.addLevelName(DEBUG_SENSOR2, "SENSOR2")
    logging.addLevelName(DEBUG_SENSOR3, "SENSOR3")
    logging.addLevelName(DEBUG_SENSOR4, "SENSOR4")
    logging.addLevelName(DEBUG_SENSOR5, "SENSOR5")

    def sensor1(self, message, *args, **kws):
        # Yes, logger takes its '*args' as 'args'.
        if self.isEnabledFor(DEBUG_SENSOR1):
            self._log(DEBUG_SENSOR1, message, args, **kws)

    def sensor2(self, message, *args, **kws):
        # Yes, logger takes its '*args' as 'args'.
        if self.isEnabledFor(DEBUG_SENSOR2):
            self._log(DEBUG_SENSOR2, message, args, **kws)

    def sensor3(self, message, *args, **kws):
        # Yes, logger takes its '*args' as 'args'.
        if self.isEnabledFor(DEBUG_SENSOR3):
            self._log(DEBUG_SENSOR3, message, args, **kws)

    def sensor4(self, message, *args, **kws):
        # Yes, logger takes its '*args' as 'args'.
        if self.isEnabledFor(DEBUG_SENSOR4):
            self._log(DEBUG_SENSOR4, message, args, **kws)

    def sensor5(self, message, *args, **kws):
        # Yes, logger takes its '*args' as 'args'.
        if self.isEnabledFor(DEBUG_SENSOR5):
            self._log(DEBUG_SENSOR5, message, args, **kws)

    logging.Logger.sensor1 = sensor1
    logging.Logger.sensor2 = sensor2
    logging.Logger.sensor3 = sensor3
    logging.Logger.sensor4 = sensor4
    logging.Logger.sensor5 = sensor5


class SensorValues(resource.Resource):
    '''Handles other/sensor_values requests'''
    def __init__(self):
        super(SensorValues, self).__init__()

        # Add Custom level to logging
        set_custom_log_levels()

        async_logdb = AsyncPostgresHandler()
        logging.getLogger('coap-server').addHandler(async_logdb)

    def register(self, resourceRoot):
        '''Registers URI with aiocoap'''
        resourceRoot.add_resource(('other', 'sensor-values'), self);
        return

    async def render_post(self, request):
        '''Sends incoming data to database'''
        original_payload = loads(request.payload)
        
        payload = dict(original_payload)

        key = payload.get('key')

        if key is None:
            key = uuid.uuid4()
        else:
            del payload['key']
            
        # Demonstrate IntegrityError
        key = 'same'
        conf = Configurator()
        async with aiopg.sa.create_engine(conf.db_conn_str) as engine:
            async with engine.acquire() as conn:
                try:
                    await conn.execute(__tbl__.insert().values(data=payload, key=key))
                except psycopg2.IntegrityError as e:
                    logging.getLogger('coap-server').exception(
                        "Duplicate key (%s) inserting sensor values: %s"
                        , key # Maybe the generated key is the problem
                        , original_payload)

        logging.getLogger('coap-server').log(DEBUG_SENSOR1, 'Inserted Sensor Values: %s, %s', key, payload)

        return aiocoap.Message(payload=''.encode("utf-8"))

