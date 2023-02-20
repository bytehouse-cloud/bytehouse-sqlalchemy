# ByteHouse SQLAlchemy Connector
## Introduction
ByteHouse SQLAlchemy connector is ByteHouse dialect for `SQLAlchemy`, which is the Python SQL toolkit and object 
relational mapper enabling application developers the full power and flexibility of SQL. The connector is built on 
top of ByteHouse python driver which follows Python DB API 2.0 specification. The connector supports both SQLAlchemy 
Core and SQLAlchemy ORM APIs. 
## Requirements
Python v3.6 or higher
## Installation from PyPI
The latest release version can be installed from here:
```commandline
pip install bytehouse-sqlalchemy
```
## Installation from github
The current development version can be installed from here:
```commandline
pip install git+https://github.com/bytehouse-cloud/bytehouse-sqlalchemy@master#egg=bytehouse-driver
```
## Creating ByteHouse Account
You need to create a ByteHouse account in order to use Python Driver. You can simply create a free account with 
the process mentioned in our official website documentation: https://docs.bytehouse.cloud/en/docs/quick-start <br/>

You can also create ByteHouse account through Volcano Engine by ByteDance: https://www.volcengine.com/product/bytehouse-cloud
## SQLAlchemy APIs
SQLAlchemy has two distinct APIs, one building on top of the other. These APIs are `Core` and `ORM`. 
### SQLAlchemy Core
SQLAlchemy Core APIs manage connectivity to a database, interact with database queries and results & manage programmatic
construction of SQL statements. 
#### Engine Configuration
`Engine` is the starting point of any SQLAlchemy application. `Engine` refers to a `Dialect` and `Connection Pool`, where 
`Dialect` is a `Python` object that represents information and methods that allow database operations to proceed on a 
particular kind of database backend and a particular kind of Python driver for that database.  <br/><br/>
The engine and its underlying connection pool do not establish the first actual DBAPI connection until the 
`Engine.connect()` method is called, or an operation which is dependent on this method such as `Engine.execute()` is 
invoked. In this way, `Engine` and `Pool` can be said to have lazy initialization behaviour.
##### ByteHouse Regions
Currently, the driver supports the following region names across different cloud providers. Alternatively, if you know
the host address of ByteHouse server, you can directly use host address & omit region name. 
<table>
    <tr>
        <td>Region Name</td>
        <td>Target Server</td>
    </tr>
    <tr>
        <td>AP-SOUTHEAST-1</td>
        <td>gateway.aws-ap-southeast-1.bytehouse.cloud:19000</td>
    </tr>
    <tr>
        <td>VOLCANO-CN-NORTH-1</td>
        <td>bytehouse-cn-beijing.volces.com:19000</td>
    </tr>
</table>

##### Construction from ByteHouse URI
###### Region & Password Format
*Required parameters:* `region` `account` `user` `password`
```python
from sqlalchemy import create_engine

engine = create_engine("bytehouse:///?region={}&account={}&user={}&password={}&database={}".
    format($REGION, $ACCOUNT, $USER, $PASSWORD, $DATABASE))
```
###### Host Address & Password Format
*Required parameters:* `host` `port` `account` `user` `password`
```python
from sqlalchemy import create_engine

engine = create_engine("bytehouse://{}:{}/?account={}&user={}&password={}&database={}".
    format($HOST, $PORT, $ACCOUNT, $USER, $PASSWORD, $DATABASE))
```
> For API Key authentication, user is always 'bytehouse'
###### Region & API Key Format
*Required parameters:* `region` `password`
```python
from sqlalchemy import create_engine

engine = create_engine("bytehouse:///?region={}&user=bytehouse&password={}&database={}".
    format($REGION, $API_KEY, $DATABASE))
```
###### Host Address & API Key Format
*Required parameters:* `host` `port` `password`
```python
from sqlalchemy import create_engine

engine = create_engine("bytehouse://{}:{}/?user=bytehouse&password={}&database={}".
    format($HOST, $PORT, $API_KEY, $DATABASE))
```
##### Programmatic Construction
```python
from sqlalchemy.engine import URL

uri = URL.create(
    "bytehouse",
    username="bytehouse",
    password="{}".format($API_KEY),
    host="{}".format($HOST),
    port="{}".format($PORT),
    database="{}".format($DATABASE),
)
engine = create_engine(uri)
```
#### Working with Connections
The most basic function of the `Engine` is to provide access to a `Connection`, which can execute SQL statements. 
To execute a textual statement to the database looks like:
```python
from sqlalchemy import text

with engine.connect() as connection:
    result = connection.execute(text("SELECT 1"))
    for row in result:
        print(row[0])
```
The object returned here is known as the `CursorResult`, which refers to a DBAPI cursor. The DBAPI cursor will be 
closed by the `CursorResult` when all of its result rows are exhausted. When the `Connection` is closed at the end of 
the with block, the referenced DBAPI connection is released to the connection pool. 
#### Working with Transactions
The `Connection` object provides a `Connection.begin()` method which returns a `Transaction` object. The transaction is 
committed when the block completes. If an exception is raised, the transaction would be rolled back, and the exception 
would be propagated outwards. 
```python
with engine.connect() as connection:
    with connection.begin():
        connection.execute(user_table.insert(), {"user_id": 7, "user_name": "Jane"})
        connection.execute(user_table.insert(), {"user_id": 8, "user_name": "Adam"})
```
#### Database MetaData
`MetaData` is a container object that keeps together different entities or features of a database. 
```python
from sqlalchemy import MetaData

metadata_obj = MetaData()
```
#### Table Definition : Constructor Style
`Table` class would represent a table where two primary arguments are the table name and the `MetaData` object which 
it will be associated with. The remaining positional arguments are Column objects describing each `Column` and engine
(`CnchMergeTree`) definition.
```python
from sqlalchemy import Table, Column, Integer, String, func
from bytehouse_sqlalchemy import engines

user_table = Table(  
    "user",  
    metadata,  
    Column("user_id", Integer, primary_key=True),  
    Column("user_name", String(16), nullable=False),  
    engines.CnchMergeTree(  
        order_by=func.tuple()  
    )  
)
```
#### Creating and Dropping Tables
The general way of creating all tables is to execute the `create_all()` method on the `MetaData` object. This method will 
first check the existence of each individual table, and if not found, then execute `CREATE` statements for all tables. 
Similarly, for dropping all tables, we can execute the `drop_all()` method on the MetaData object. Creating and dropping
individual tables can be done via the `create()` and `drop()` methods. 
```python
from sqlalchemy import Table, Column, Integer, String, func, MetaData
from bytehouse_sqlalchemy import engines

metadata_obj = MetaData()
user_table = Table(  
    "user",  
    metadata,  
    Column("user_id", Integer, primary_key=True),  
    Column("user_name", String(16), nullable=False),  
    engines.CnchMergeTree(  
        order_by=func.tuple()  
    )  
)
metadata_obj.create_all(engine)
metadata_obj.drop_all(engine)
```
#### Insertion and Selection
`Table.insert()` can be used to insert rows into the table, whereas `Table.select()` would fetch the result rows from the server.
```python
with engine.connect() as connection:
    connection.execute(user_table.insert(), {"user_id": 7, "user_name": "Jane"})
    result_set = connection.execute(user_table.select())
```
### SQLAlchemy ORM
SQLAlchemy ORM is built on top of SQLAlchemy Core which provides object relational mapping capabilities that allows 
users to define Python classes mapped to database tables. It extends the Core SQL expression language to allow 
SQL queries to be composed and invoked in user defined objects. 
#### Create Engine
The `Engine` is a factory class that will create and maintain database connections for us, where connections are held 
inside of a Connection Pool for fast reuse. The details regarding the `Engine` are described at the beginning of this doc. 
```python
from sqlalchemy import create_engine

engine = create_engine("bytehouse:///?region={}&account={}&user={}&password={}&database={}".
    format($REGION, $ACCOUNT, $USER, $PASSWORD, $DATABASE))
```
#### Declarative Mapping
The `Declarative Mapping` defines a base class using the `declarative_base()` function, which returns a new base class 
from which new classes to be mapped may inherit from. A mapped class typically refers to a single particular database 
table, the name of which is indicated by using the `__tablename__` class level attribute.
```python
from sqlalchemy import Column, Integer, String, func, MetaData
from sqlalchemy.orm import declarative_base
from bytehouse_sqlalchemy import engines

metadata = MetaData(bind=engine)
Base = declarative_base(metadata=metadata)

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
```
#### Table Creation
Using table metadata and engine, we can generate DDL schema & execute in ByteHouse at once using 
`Table.__table__.create()` method. 
```python
User.__table__.create()
```
#### Session and Object Persist
We can create objects of previously defined classes and pass them to the database using an object called `Session`, 
which uses the `Engine` to interact with the database. The `Session.add_all()` is used to add multiple objects at once, 
and the `Session.commit()` method would flush any pending changes to the database.
```python
from sqlalchemy.orm import Session

with Session(engine) as session:
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
        session.add_all([spongebob, sandy])  
        session.commit()
```
#### SELECT statement
We can use the `select()` method to create a new `Select` object, which can then be invoked using a `Session` object. 
Optionally, we can also use the `Select.where()` method to filter out the results.
```python
from sqlalchemy import select

session = Session(engine)

stmt = select(User).where(User.name.in_(["spongebob", "sandy"]))
for user in session.scalars(stmt):  
    print(user.name)
```
## Local Development
Change `setup.cfg` file to include your connection credentials. For running tests locally, follow these steps:
```commandline
python testsrequire.py && pip install .
python -m pytest tests/
```
## License
This project is distributed under the terms of the MIT license: http://www.opensource.org/licenses/mit-license.php