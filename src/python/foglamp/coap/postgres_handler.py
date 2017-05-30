import time
import logging

import asyncio
import sqlalchemy as sa
from foglamp.configurator import Configurator
import logging.handlers

metadata = sa.MetaData()

__log__ = sa.Table(
    'log_t'
    , metadata
    , sa.Column('log_level', sa.types.INT)
    , sa.Column('log_levelname', sa.types.VARCHAR(10))
    , sa.Column('log', sa.types.VARCHAR(100))
    , sa.Column('created_at', sa.types.DATE)
    , sa.Column('created_by', sa.types.VARCHAR(50)))
'''Record Log data into this table'''

class PostgresHandler(logging.Handler):
    '''Customized logging handler that puts logs to the postgres database'''

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
            conn.execute(__log__.insert().values(log_level=record.levelno, \
                                                       log_levelname=record.levelname, \
                                                       log=self.log_msg, \
                                                       created_at=tm, \
                                                       created_by=record.name))
        except Exception:
            self.handleError(record)

    def emit(self, record):
        return self._emit(record)


class AsyncPostgresHandler(PostgresHandler):
    '''Customized logging handler that puts logs to the postgres database'''

    def __init__(self):
        super().__init__()

    async def __emit(self, record):
        self._emit(record)

    def emit(self, record):
        asyncio.get_event_loop().run_until_complete(self.__emit(record))


# from log_pg_handler import *

if __name__ == '__main__':
    logdb = AsyncPostgresHandler()
    logging.getLogger('').addHandler(logdb)
    log = logging.getLogger("my_logger")
    log.error('This is a test error')
