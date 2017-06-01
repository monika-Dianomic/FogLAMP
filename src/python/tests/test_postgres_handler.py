import logbook
import pytest
import logging
import asyncio
import sqlalchemy as sa
from sqlalchemy.engine import create_engine
from sqlalchemy.orm.session import Session
from foglamp.configurator import Configurator
from foglamp.postgres_handler import *
from foglamp.foglamp_logger import *

# noinspection PyClassHasNoInit


class TestPostgresHandler:
    connection = engine = conf = meta = None

    @classmethod
    def setup_class(cls):
        cls.conf = Configurator()
        cls.conf.FOGLAMP_DEPLOYMENT = 'test'
        cls.conf.initialize_dbconfig(cls.conf.FOGLAMP_DEPLOYMENT)

        def check_for_tables():
            # Connect to the database and create the schema within a transaction
            engine = sa.create_engine(cls.conf.db_conn_str)
            connection = engine.connect()
            meta = sa.MetaData(bind=connection, reflect=True)
            with connection  as conn:
                if 'log_t' not in meta.tables:
                    _log_tbl = sa.Table(
                        'log_t'
                        , meta
                        , sa.Column('log_level', sa.types.INT)
                        , sa.Column('log_levelname', sa.types.VARCHAR(10))
                        , sa.Column('log', sa.types.VARCHAR(100))
                        , sa.Column('created_at', sa.types.DATE)
                        , sa.Column('created_by', sa.types.VARCHAR(50)))
                    _log_tbl.create()

        check_for_tables()

        cls.engine = sa.create_engine(cls.conf.db_conn_str)
        cls.connection = cls.engine.connect()
        cls.meta = sa.MetaData(bind=cls.connection, reflect=True)

    @classmethod
    def teardown_class(cls):
        cls.connection.close()
        cls.engine.dispose()

    def setup_method(self, method):
        self.__transaction = TestPostgresHandler.connection.begin()
        self.session = Session(TestPostgresHandler.connection)

    def teardown_method(self, method):
        # TODO: Investigate why rollback is not workingfrom foglamp.foglamp_logger import *

        self.__transaction.rollback()

        logs = TestPostgresHandler.meta.tables['log_t']
        self.session.execute(logs.delete())
        self.session.commit()
        self.session.close()

    def test_log(self):
        foglamp_logger.sensor1('This is a test error')

        logs = TestPostgresHandler.meta.tables['log_t']
        for row in TestPostgresHandler.connection.execute(logs.select()):
            assert row.log_level == 51
            assert row.log_levelname == 'SENSOR1'
            assert row.log == 'This is a test error'
            assert row.created_by == "foglamp.foglamp_logger"

    def test_async_log(self):
        foglamp_logger.sensor2('This is a async test error')

        logs = TestPostgresHandler.meta.tables['log_t']
        for row in TestPostgresHandler.connection.execute(logs.select()):
            assert row.log_level == 52
            assert row.log_levelname == 'SENSOR2'
            assert row.log == 'This is a async test error'
            assert row.created_by == "foglamp.foglamp_logger"
