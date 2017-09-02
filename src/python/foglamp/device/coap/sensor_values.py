# -*- coding: utf-8 -*-
"""FOGLAMP_PRELUDE_BEGIN
{{FOGLAMP_LICENSE_DESCRIPTION}}

See: http://foglamp.readthedocs.io/

Copyright (c) 2017 OSIsoft, LLC
License: Apache 2.0

FOGLAMP_PRELUDE_END
"""

import logging
from cbor2 import loads
import aiocoap
import aiocoap.resource
import psycopg2
import aiopg.sa
import sqlalchemy as sa
import asyncio
from sqlalchemy.dialects.postgresql import JSONB
from foglamp import statistics

"""CoAP handler for coap://other/sensor_readings URI
"""

__author__ = 'Terris Linenbach'
__version__ = '${VERSION}'

_sensor_values_tbl = sa.Table(
    'readings',
    sa.MetaData(),
    sa.Column('asset_code', sa.types.VARCHAR(50)),
    sa.Column('read_key', sa.types.VARCHAR(50)),
    sa.Column('user_ts', sa.types.TIMESTAMP),
    sa.Column('reading', JSONB))
"""Defines the table that data will be inserted into"""


class SensorValues(aiocoap.resource.Resource):
    """CoAP handler for coap://readings URI

        Attributes:
            _num_readings (int) : number of readings processed through render_post method since initialization or since the last time _update_statistics() was called
            _num_discarded_readings (int) : number of readings discarded through render_post method since initialization or since the last time _update_statistics() was called
    """

    _CONNECTION_STRING = "dbname='foglamp'"

    # 'postgresql://foglamp:foglamp@localhost:5432/foglamp'


    def __init__(self):
        super(SensorValues, self).__init__()
        asyncio.ensure_future(self._update_statistics())
        self._num_readings = 0
        self._num_discarded_readings = 0

    def register_handlers(self, resource_root, uri):
        """Registers other/sensor_values URI"""
        resource_root.add_resource(('other', uri), self)
        return

    async def _update_statistics(self):
        while True:
            await asyncio.sleep(5)

            try:
                readings = self._num_readings
                self._num_readings = 0
                try:
                    await statistics.update_statistics_value('READINGS', readings)
                except Exception:
                    self._num_readings += readings
                    raise
                readings = self._num_discarded_readings
                self._num_discarded_readings = 0
                try:
                    await statistics.update_statistics_value('DISCARDED', readings)
                except Exception:
                    self._num_discarded_readings += readings
                    raise
            except Exception:
                logging.getLogger('coap-server').exception(
                            'An error occurred while updating statistics')

    async def render_post(self, request):
        """Sends asset readings to storage layer

        request.payload looks like:
        {
            "timestamp": "2017-01-02T01:02:03.23232Z-05:00",
            "asset": "pump1",
            "readings": {
                "velocity": "500",
                "temperature": {
                    "value": "32",
                    "unit": "kelvin"
                }
            }
        }
        """

        # TODO: The payload format is documented
        # at https://docs.google.com/document/d/1rJXlOqCGomPKEKx2ReoofZTXQt9dtDiW_BHU7FYsj-k/edit#
        # and will be moved to a .rst file

        self._num_readings += 1

        # Required keys in the payload
        try:
            payload = loads(request.payload)
            asset = payload['asset']
            timestamp = payload['timestamp']
        except:
            self._num_discarded_readings += 1
            return aiocoap.Message(payload=''.encode("utf-8"), code=aiocoap.numbers.codes.Code.BAD_REQUEST)

        # Optional keys in the payload
        readings = payload.get('sensor_values', {})
        key = payload.get('key')

        # Comment out to test IntegrityError
        # key = '123e4567-e89b-12d3-a456-426655440000'

        try:
            async with aiopg.sa.create_engine(self._CONNECTION_STRING) as engine:
                async with engine.acquire() as conn:
                    try:
                        await conn.execute(_sensor_values_tbl.insert().values(
                            asset_code=asset, reading=readings, read_key=key, user_ts=timestamp))
                    except psycopg2.IntegrityError:
                        logging.getLogger('coap-server').exception(
                            'Duplicate key (%s) inserting sensor values:\n%s',
                            key,
                            payload)
        except Exception:
            logging.getLogger('coap-server').exception(
                "Database error occurred. Payload:\n%s"
                , payload)
            return aiocoap.Message(payload=''.encode("utf-8"), code=aiocoap.numbers.codes.Code.INTERNAL_SERVER_ERROR)

        return aiocoap.Message(payload=''.encode("utf-8"), code=aiocoap.numbers.codes.Code.VALID)
        # TODO what should this return?
