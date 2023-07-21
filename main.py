import sqlalchemy as sq
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
import json
from pprint import pprint

import db_conn

drivername = "postgresql"
driver = {"postgresql": {"server_name": "localhost",
                         "server_port": 5432}}
db = "postgres"

Base = declarative_base()

# Homework 1
class Publisher(Base):
    __tablename__ = "publisher"

    id = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String(length=40), unique=True)

    def __str__(self):
        return f'Publisher {self.id}: {self.name}'

class Shop(Base):
    __tablename__ = "shop"

    id = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String(length=40), unique=True)

    def __str__(self):
        return f'Shop {self.id}: {self.name}'

class Book(Base):
    __tablename__ = "book"

    id = sq.Column(sq.Integer, primary_key=True)
    title = sq.Column(sq.String(length=40), nullable=False)
    id_publisher = sq.Column(sq.Integer, sq.ForeignKey("publisher.id"), nullable=False)

    publisher = relationship(Publisher, backref="book")

    def __str__(self):
        return f'Book {self.id}: ({self.title}, {self.id_publisher})'

class Stock(Base):
    __tablename__ = "stock"

    id = sq.Column(sq.Integer, primary_key=True)
    id_book = sq.Column(sq.Integer, sq.ForeignKey("book.id"), nullable=False)
    id_shop = sq.Column(sq.Integer, sq.ForeignKey("shop.id"), nullable=False)
    count = sq.Column(sq.Integer, nullable=False)

    book = relationship(Book, backref="stock")
    shop = relationship(Shop, backref="stock")

    def __str__(self):
        return f'Stock {self.id}: ({self.id_book}, {self.id_shop}, {self.count})'

class Sale(Base):
    __tablename__ = "sale"

    id = sq.Column(sq.Integer, primary_key=True)
    price = sq.Column(sq.Float, nullable=False)
    date_sale = sq.Column(sq.Date, nullable=False)
    id_stock = sq.Column(sq.Integer, sq.ForeignKey("stock.id"), nullable=False)
    count = sq.Column(sq.Integer, nullable=False)

    stock = relationship(Stock, backref="sale")

    def __str__(self):
        return f'Sale {self.id}: ({self.price}, {self.date_sale}, {self.id_stock}, {self.count})'

def create_tables(engine):
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

DSN = f"{drivername}://{db_conn.user}:{db_conn.password}@{driver[drivername]['server_name']}:{driver[drivername]['server_port']}/{db}"
engine = sq.create_engine(DSN)
create_tables(engine)

# Homework 3

Session = sessionmaker(bind=engine)
session = Session()

with open("fixtures/tests_data.json", encoding = "utf-8") as f:
    json_data = json.load(f)

for el in json_data:
    model = {
        'publisher':Publisher,
        'shop': Shop,
        'book': Book,
        'stock': Stock,
        'sale': Sale,
        }[el.get('model')]
    session.add(model(id=el.get('pk'),**el.get('fields')))
session.commit()

# Homework 2
publisher=input("Введите имя или идентификатор издателя - ")
if publisher.isdigit():
    publisher = int(publisher)

if type(publisher) == int:
    subq1 = session.query(Publisher).filter(Publisher.id == publisher).subquery()
else:
    subq1 = session.query(Publisher).filter(Publisher.name.like('%' + publisher + '%')).subquery()
subq2 = session.query(Book).join(subq1, Book.id_publisher == subq1.c.id).subquery()
subq3 = session.query(Stock).join(subq2, Stock.id_book == subq2.c.id).join(Stock.shop).subquery()

print('название книги | название магазина, в котором была куплена эта книга | стоимость покупки | дата покупки')
for el in session.query(Sale).join(subq3, Sale.id_stock == subq3.c.id):
    print(el.stock.book.title, "|", el.stock.shop.name, "|", el.price, "|", el.date_sale)

session.close()