import logging
import uuid
import aiocoap
import aiocoap.resource as resource
import aiopg.sa
import psycopg2
import sqlalchemy as sa
from cbor2 import loads
from foglamp.configurator import Configurator
from foglamp.postgres_handler import AsyncPostgresHandler
from sqlalchemy.dialects.postgresql import JSONB

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

def set_custom_log_levels():
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

