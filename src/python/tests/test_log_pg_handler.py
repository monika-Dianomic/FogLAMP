import pytest
from sqlalchemy.engine import create_engine
from sqlalchemy.orm.session import Session
from foglamp.configurator import Configurator

def setup_module():
    global transaction, connection, engine

    # Connect to the database and create the schema within a transaction
    engine = create_engine('postgresql:///yourdb')
    connection = engine.connect()
    transaction = connection.begin()
    Base.metadata.create_all(connection)

    # If you want to insert fixtures to the DB, do it here


def teardown_module():
    # Roll back the top level transaction and disconnect from the database
    transaction.rollback()
    connection.close()
    engine.dispose()


class DatabaseTest(object):
    def setup(self):
        self.__transaction = connection.begin_nested()
        self.session = Session(connection)

    def teardown(self):
        self.session.close()
        self.__transaction.rollback()
