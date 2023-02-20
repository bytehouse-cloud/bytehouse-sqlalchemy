from unittest import TestCase
import configparser

from sqlalchemy import create_engine, Column, MetaData, func, Integer, String, text, MetaData
from sqlalchemy.engine import URL

from bytehouse_sqlalchemy import (
    Table, make_session, get_declarative_base, types, engines, select
)


class DocumentationTestCase(TestCase):
    file_config = configparser.ConfigParser()
    file_config.read(['setup.cfg'])

    host = file_config.get('db', 'host')
    port = file_config.getint('db', 'port')
    database = file_config.get('db', 'database')
    account = file_config.get('db', 'account')
    user = file_config.get('db', 'user')
    password = file_config.get('db', 'password')
    api_key = file_config.get('db', 'api_key')
    region = file_config.get('db', 'region')

    def test_region_account_auth(self):
        uri = 'bytehouse:///?region={}&account={}&user={}&password={}&database={}'. \
            format(self.region, self.account, self.user, self.password, self.database)
        result = create_engine(uri).connect().execute("SELECT 1").fetchall()[0][0]
        self.assertEqual(result, 1)

    def test_host_port_account_auth(self):
        uri = 'bytehouse://{}:{}/?account={}&user={}&password={}&database={}'. \
            format(self.host, self.port, self.account, self.user, self.password, self.database)
        result = create_engine(uri).connect().execute("SELECT 1").fetchall()[0][0]
        self.assertEqual(result, 1)

    def test_region_api_key(self):
        uri = 'bytehouse:///?region={}&user=bytehouse&password={}&database={}'. \
            format(self.region, self.api_key, self.database)
        result = create_engine(uri).connect().execute("SELECT 1").fetchall()[0][0]
        self.assertEqual(result, 1)

    def test_host_port_api_key(self):
        uri = 'bytehouse://{}:{}/?user=bytehouse&password={}&database={}'. \
            format(self.host, self.port, self.api_key, self.database)
        result = create_engine(uri).connect().execute("SELECT 1").fetchall()[0][0]
        self.assertEqual(result, 1)

    def test_programmatic_uri_construction(self):
        uri = URL.create(
            "bytehouse",
            username="bytehouse",
            password="{}".format(self.api_key),
            host="{}".format(self.host),
            port="{}".format(self.port),
            database="{}".format(self.database),
        )
        result = create_engine(uri).connect().execute("SELECT 1").fetchall()[0][0]
        self.assertEqual(result, 1)

    def test_textual_emit(self):
        uri = 'bytehouse:///?region={}&user=bytehouse&password={}&database={}'. \
            format(self.region, self.api_key, self.database)
        engine = create_engine(uri)
        with engine.connect() as connection:
            result = connection.execute(text("SELECT 1"))
            for row in result:
                self.assertEqual(row[0], 1)

    def test_transaction_commit(self):
        uri = 'bytehouse:///?region={}&user=bytehouse&password={}&database={}'. \
            format(self.region, self.api_key, self.database)
        engine = create_engine(uri)
        metadata = MetaData()
        user_table = Table(
            "user",
            metadata,
            Column("user_id", Integer, primary_key=True),
            Column("user_name", String(16), nullable=False),
            engines.CnchMergeTree(
                order_by=func.tuple()
            )
        )
        try:
            metadata.create_all(engine)
            with engine.connect() as connection:
                with connection.begin():
                    connection.execute(user_table.insert(), {"user_id": 7, "user_name": "Jane"})
                    connection.execute(user_table.insert(), {"user_id": 8, "user_name": "Adam"})
                result_set = connection.execute(user_table.select()).fetchall()
                self.assertEqual(result_set[0][0], 7)
                self.assertEqual(result_set[1][0], 8)
        finally:
            with engine.connect() as connection:
                connection.execute("DROP TABLE IF EXISTS {}.user".format(self.database))

    def test_declarative_models(self):
        uri = 'bytehouse:///?region={}&user=bytehouse&password={}&database={}'. \
            format(self.region, self.api_key, self.database)
        engine = create_engine(uri)
        session = make_session(engine)
        metadata = MetaData(bind=engine)
        Base = get_declarative_base(metadata=metadata)

        session.execute("DROP TABLE IF EXISTS user_account")

        class User(Base):
            __tablename__ = "user_account"
            id = Column(Integer, primary_key=True)
            name = Column(String(30))
            fullname = Column(String)

            __table_args__ = (
                engines.CnchMergeTree(
                    order_by=func.tuple()
                ),
            )

        spongebob = User(
            id=1,
            name="spongebob",
            fullname="Spongebob Squarepants"
        )
        sandy = User(
            id=2,
            name="sandy",
            fullname="Sandy Cheeks"
        )

        User.__table__.create()

        session.add_all([spongebob, sandy])
        session.commit()

        stmt = select(User).where(User.name.in_(["spongebob", "sandy"]))

        names = ["spongebob", "sandy"]
        for user in session.scalars(stmt):
            assert user.name in names
