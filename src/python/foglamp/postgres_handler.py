import time
import logging
import asyncio
import sqlalchemy as sa
import aiopg.sa
import psycopg2
import logging.handlers
from foglamp.configurator import Configurator
from aiopg.sa import create_engine

metadata = sa.MetaData()

log_table = sa.Table(
    'log_t'
    , metadata
    , sa.Column('log_level', sa.types.INT)
    , sa.Column('log_levelname', sa.types.VARCHAR(10))
    , sa.Column('log', sa.types.VARCHAR(100))
    , sa.Column('created_at', sa.types.DATE)
    , sa.Column('created_by', sa.types.VARCHAR(50)))
'''Record Log data into this table'''

class PostgresHandler(logging.Handler):
    """Customized logging handler that puts logs to the postgres database"""

    def __init__(self):
        logging.Handler.__init__(self)
        Configurator().initialize_dbconfig()

    def _emit(self, record):
        tm = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(record.created))
        self.log_msg = record.msg
        self.log_msg = self.log_msg.strip()
        self.log_msg = self.log_msg.replace('\'', '\'\'')

        conf = Configurator()
        conn = sa.create_engine(conf.db_conn_str)

        try:
            conn.execute(log_table.insert().values(log_level=record.levelno, \
                                                       log_levelname=record.levelname, \
                                                       log=self.log_msg, \
                                                       created_at=tm, \
                                                       created_by=record.name))
        except Exception:
            self.handleError(record)

    def emit(self, record):
        return self._emit(record)


class AsyncPostgresHandler(logging.Handler):
    """Customized asynchronous logging handler that puts logs to the postgres database"""

    def __init__(self):
        logging.Handler.__init__(self)
        Configurator().initialize_dbconfig()

    async def _emit(self, record):
        tm = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(record.created))
        self.log_msg = record.msg
        self.log_msg = self.log_msg.strip()
        self.log_msg = self.log_msg.replace('\'', '\'\'')

        conf = Configurator()
        async with aiopg.sa.create_engine(conf.db_conn_str) as engine:
            async with engine.acquire() as conn:
                try:
                    await conn.execute(log_table.insert().values(log_level=record.levelno, \
                                                     log_levelname=record.levelname, \
                                                     log=self.log_msg, \
                                                     created_at=tm, \
                                                     created_by=record.name))
                except Exception:
                    self.handleError(record)

    def emit(self, record):
        asyncio.get_event_loop().run_until_complete(self._emit(record))
