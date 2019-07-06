import datetime

import flask_sqlalchemy
from flask import Flask, jsonify, render_template
from flask_pymongo import PyMongo
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import text
from flask_bootstrap import Bootstrap
from flask_debugtoolbar import DebugToolbarExtension
from flask_pymongo import PyMongo
from py2neo import Graph, Node, Relationship, NodeMatcher, RelationshipMatcher

database_server_name = 'SQL2017'
mssql_host = 'localhost'
mssql_user = 'sa'
mssql_pwd = 'ssk07@am'
mssql_db = 'InstaCart'
mssql_driver = 'pymssql'

SECRET_KEY = '19452559da0a7914704a66f117186412'
SQLALCHEMY_DATABASE_URI = 'mssql+{0}://{1}:{2}@{3}\{4}/{5}'.format(
    mssql_driver,
    mssql_user,
    mssql_pwd,
    mssql_host,
    database_server_name,
    mssql_db)

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = SQLALCHEMY_DATABASE_URI
db = SQLAlchemy(app)
bootstrap = Bootstrap(app)

app.debug = True
app.config['MONGO_DBNAME'] = "test"
app.config["MONGO_URI"] = "mongodb://127.0.0.1:27017/test"

mongo = PyMongo(app)

# set a 'SECRET_KEY' to enable the Flask session cookies
app.config['SECRET_KEY'] = 'INFSCI2711'

toolbar = DebugToolbarExtension(app)

app.config['SQLALCHEMY_RECORD_QUERIES'] = True


class Aisle(db.Model):
    __tablename__ = 'Aisle'
    AisleId = db.Column(db.Integer, primary_key=True)
    Aisle = db.Column(db.String(50))


class Department(db.Model):
    __tablename__ = 'Department'
    DepartmentId = db.Column(db.Integer, primary_key=True)
    Department = db.Column(db.String(50))


class Order(db.Model):
    __tablename__ = 'Order'
    OrderId = db.Column(db.Integer, primary_key=True)
    UserId = db.Column(db.Integer, primary_key=True)
    Evaluation = db.Column(db.String(50))
    OrderNumber = db.Column(db.Integer)
    DayOfWeek = db.Column(db.Integer)
    Hour = db.Column(db.Integer)
    SinceLast = db.Column(db.Integer)


class OrderDetails(db.Model):
    __tablename__ = 'OrderDetails'
    OrderId = db.Column(db.Integer, primary_key=True)
    ProductId = db.Column(db.Integer, primary_key=True)
    CartOrder = db.Column(db.Integer)
    Reordered = db.Column(db.Integer)


class OrderUserStarFact(db.Model):
    __tablename__ = 'OrderUserStarFact'
    OrderId = db.Column(db.Integer, primary_key=True)
    UserId = db.Column(db.Integer)
    Quantity = db.Column(db.Integer)


class Product(db.Model):
    __tablename__ = 'Product'
    ProductId = db.Column(db.Integer, primary_key=True)
    Name = db.Column(db.String(50))
    AisleId = db.Column(db.Integer)
    DepartmentId = db.Column(db.Integer)


class ProductDayOfWeekHourStarFact(db.Model):
    __tablename__ = 'ProductDayOfWeekHourStarFact'
    ProductId = db.Column(db.Integer, primary_key=True)
    DayOfWeek = db.Column(db.Integer, primary_key=True)
    Hour = db.Column(db.Integer, primary_key=True)
    Quantity = db.Column(db.Integer)


class ProductStarFact(db.Model):
    __tablename__ = 'ProductStarFact'
    ProductId = db.Column(db.Integer, primary_key=True)
    DepartmentId = db.Column(db.Integer)
    AisleId = db.Column(db.Integer)
    Quantity = db.Column(db.Integer)


class ProductUserStarFact(db.Model):
    __tablename__ = 'ProductUserStarFact'
    ProductId = db.Column(db.Integer, primary_key=True)
    UserId = db.Column(db.Integer, primary_key=True)
    Quantity = db.Column(db.Integer)


class OrderByDepartment(db.Model):
    Department = db.Column(db.String, primary_key=True)
    NumberOfOrders = db.Column(db.Integer)


class MaxOrderByUser(db.Model):
    Id = db.Column(db.String, primary_key=True)
    UserId = db.Column(db.String, primary_key=True)
    Quantity = db.Column(db.Integer)


class QuantityByTimeOfDay(db.Model):
    TimeOfDay = db.Column(db.String, primary_key=True)
    Quantity = db.Column(db.Integer)


class QuantityByHour(db.Model):
    Hour = db.Column(db.Integer, primary_key=True)
    Quantity = db.Column(db.Integer)


class ProductByDayOfWeek(db.Model):
    DayOfWeek = db.Column(db.String, primary_key=True)
    Name = db.Column(db.String)
    TotalQuantity = db.Column(db.Integer)


class QuantityByAisle(db.Model):
    Aisle = db.Column(db.String, primary_key=True)
    Quantity = db.Column(db.Integer)


@app.route('/')
def hello_world():
    return render_template('home.html')


@app.route('/department/orders')
def order_by_department():
    query = u"SELECT Department, SUM([A].[Quantity]) AS NumberOfOrders " \
            u"FROM ProductStarFact AS [A] " \
            u"INNER JOIN [Department] AS [B] " \
            u"ON [A].[DepartmentId] = [B].[DepartmentId] " \
            u"GROUP BY [B].[Department] " \
            u"ORDER BY NumberOfOrders DESC;"

    orders_by_department = db.session.query(OrderByDepartment).from_statement(text(query)).all()
    query_stat = flask_sqlalchemy.get_debug_queries()
    return render_template('orders_by_department.html', orders_by_department=orders_by_department,
                           query_stat=query_stat[0])


@app.route('/department/users')
def max_orders_by_user():
    query = u"SELECT TOP(5) NEWID() AS [Id], [A].[UserId], " \
            u"SUM([A].[Quantity]) AS [Quantity] " \
            u"FROM OrderUserStarFact [A] " \
            u"GROUP BY [A].[UserId] " \
            u"ORDER BY [Quantity] DESC;"

    max_orders_by_user_result = db.session.query(MaxOrderByUser).from_statement(text(query)).all()
    query_stat = flask_sqlalchemy.get_debug_queries()[0]
    return render_template('max_orders_by_user.html', max_orders_by_user=max_orders_by_user_result,
                           query_stat=query_stat)


@app.route("/product/timeofday")
def quantity_by_time_of_day():
    query = u"SELECT [TimeOfDay], SUM(Quantity) AS [Quantity] " \
            u"FROM (SELECT CASE WHEN [Hour] >= 0 AND  [Hour] <=3 THEN '00:00 – 03:00' " \
            u"WHEN [Hour] >= 4 AND  [Hour] <= 6 THEN '03:00 – 06:00' " \
            u"WHEN [Hour] >= 7 AND  [Hour] <=9 THEN '06:00 – 09:00' " \
            u"WHEN [Hour] >= 10 AND [Hour] <=12 THEN '09:00 – 12:00' " \
            u"WHEN [Hour] >= 13 AND [Hour] <=15 THEN '12:00 – 15:00' " \
            u"WHEN [Hour] >= 16 AND [Hour] <=18 THEN '15:00 – 18:00' " \
            u"WHEN [Hour] >= 19 AND [Hour] <=21 THEN '18:00 – 21:00' " \
            u"WHEN [Hour] >= 22 AND [Hour] <=24 THEN '21:00 – 24:00'END AS [TimeOfDay], " \
            u"SUM(Quantity) AS [Quantity] " \
            u"FROM ProductDayOfWeekHourStarFact GROUP BY [Hour]) AS [A] " \
            u"GROUP BY [TimeOfDay]"
    quantity_by_time_of_day_results = db.session.query(QuantityByTimeOfDay).from_statement(text(query)).all()
    query_stat = flask_sqlalchemy.get_debug_queries()[0]
    return render_template('products_by_time_of_day.html', quantity_by_time_of_day=quantity_by_time_of_day_results,
                           query_stat=query_stat)


@app.route("/product/top20")
def product_top_twenty():
    query = u"SELECT [Hour],COUNT(*) AS [Quantity] FROM [Order] GROUP BY [Hour] ORDER BY [Quantity] DESC"
    product_top_twenty_results = db.session.query(QuantityByHour).from_statement(text(query)).all()
    query_stat = flask_sqlalchemy.get_debug_queries()[0]
    return render_template('product_top_twenty.html', product_top_twenty=product_top_twenty_results,
                           query_stat=query_stat)


@app.route("/product/aisle")
def product_quantity_by_aisle():
    query = u"SELECT " \
            u"[B].[Aisle], SUM([A].[Quantity]) AS [Quantity] " \
            u"FROM [ProductStarFact] AS [A] " \
            u"INNER JOIN [Aisle] AS [B] " \
            u"ON [A].[AisleId] = [B].[AisleId] " \
            u"GROUP BY [B].[Aisle] " \
            u"ORDER BY [Quantity]"
    product_quantity_by_aisle_results = db.session.query(QuantityByAisle).from_statement(text(query)).all()
    query_stat = flask_sqlalchemy.get_debug_queries()[0]
    return render_template('product_quantity_by_aisle.html',
                           product_quantity_by_aisle=product_quantity_by_aisle_results, query_stat=query_stat)


@app.route("/product/dayofweek")
def products_by_day_of_week():
    query = u"SELECT CASE [A].[DayOfWeek] " \
            u"WHEN 0 THEN 'Sunday' " \
            u"WHEN 1 THEN 'Monday' " \
            u"WHEN 2 THEN 'Tuesday' " \
            u"WHEN 3 THEN 'Wednesday' " \
            u"WHEN 4 THEN 'Thursday' " \
            u"WHEN 5 THEN 'Friday' " \
            u"WHEN 6 THEN 'Saturday' END AS [DayOfWeek], [B].[Name], [A].[TotalQuantity] " \
            u"FROM ProductDayOfWeekFact AS [A] INNER JOIN [Product] AS [B] " \
            u"ON [A].[ProductId] = [B].[ProductId]"
    products_by_day_of_week_results = db.session.query(ProductByDayOfWeek).from_statement(text(query)).all()
    query_stat = flask_sqlalchemy.get_debug_queries()[0]
    return render_template('products_by_day_of_week.html', products_by_day_of_week=products_by_day_of_week_results,
                           query_stat=query_stat)


## Mongo


@app.route("/mongo/orderbyhod")
def mongo_orders_by_hour_of_day():
    order_by_hour = []
    query = ([
        {'$group': {'_id': "$order_hour_of_day", 'order_count': {'$sum': 1}}},
        {'$sort': {'order_count': -1}},
        {'$limit': 20}])
    current_time_one = datetime.datetime.now()
    document = mongo.db.orders.aggregate(query)
    current_time_two = datetime.datetime.now()
    for i in document:
        order_by_hour.append(i)
    return render_template('mongo_orderbyhod.html', orderbyhod=order_by_hour,
                           execution_time=current_time_two - current_time_one)


@app.route("/mongo/productbydow")
def mongo_order_by_dow():
    product_by_dow = []
    query = ([
        {'$group': {'_id': "$order_dow", 'total_orders': {'$sum': 1}}},
        {'$sort': {'total_orders': -1}}])
    current_time_one = datetime.datetime.now()
    document = mongo.db.orders.aggregate(query)
    current_time_two = datetime.datetime.now()
    for i in document:
        product_by_dow.append(i)
    return render_template('mongo_orderbydow.html', productbydow=product_by_dow,
                           execution_time=current_time_two - current_time_one)


@app.route("/mongo/productbyaisle")
def mongo_product_quantity_by_aisle():
    product_by_aisle = []
    query = ([
        {'$group': {'_id': "$aisle_id", 'product_count': {'$sum': 1}}},
        {'$sort': {'product_count': -1}},
        {'$limit': 20}])
    current_time_one = datetime.datetime.now()
    document = mongo.db.products.aggregate(query)
    current_time_two = datetime.datetime.now()
    for i in document:
        product_by_aisle.append(i)
    return render_template('mongo_topproductbyaisle.html', productbyaisle=product_by_aisle,
                           execution_time=current_time_two - current_time_one)


@app.route("/mongo/users_order_prod")
def mongo_top_users_order_max_product():
    top_users_order_maxprod = []
    query = ([
        {'$group': {'_id': "$user_id", 'product_count': {'$sum': 1}}},
        {'$sort': {'product_count': -1}},
        {'$limit': 5}])
    current_time_one = datetime.datetime.now()
    document = mongo.db.user_order_product.aggregate(query)
    current_time_two = datetime.datetime.now()
    for i in document:
        top_users_order_maxprod.append(i)
    return render_template('mongo_topusersordermaxproducts.html', users_order_prod=top_users_order_maxprod,
                           execution_time=current_time_two - current_time_one)


@app.route("/mongo/topproducts")
def mongo_top_products():
    top_20 = []
    query = ([
        {'$group': {'_id': "$product_name", 'product_count': {'$sum': 1}}},
        {'$sort': {'product_count': -1}},
        {'$limit': 20}])
    current_time_one = datetime.datetime.now()
    document = mongo.db.order_product_detail.aggregate(query)
    current_time_two = datetime.datetime.now()
    for i in document:
        top_20.append(i)
    return render_template('mongo_topproducts.html', topproducts=top_20,
                           execution_time=current_time_two - current_time_one)


@app.route("/mongo/orderbydepartment")
def mongo_order_department():
    orderdepartment = []
    query = ([
        {'$group': {'_id': "$department", 'product_count': {'$sum': 1}}},
        {'$sort': {'product_count': -1}}])
    current_time_one = datetime.datetime.now()
    document = mongo.db.order_product_detail.aggregate(query)
    current_time_two = datetime.datetime.now()
    for i in document:
        orderdepartment.append(i)
    return render_template('mongo_orderbydepartment.html', orderdepartments=orderdepartment,
                           execution_time=current_time_two - current_time_one)


## Neo4j

@app.route("/neo4j/product/aisle")
def neo4j_product_quantity_by_aisle():
    product_quantity_by_aisle_results = Neo4jInstacart().AisleProductQuantity()
    return render_template('neo4j_product_quantity_by_aisle.html',
                           product_quantity_by_aisle=product_quantity_by_aisle_results)


@app.route('/neo4j/department/orders')
def neo4j_order_by_department():
    orders_by_department = Neo4jInstacart().DepartmentProductQuantity()
    return render_template('neo4j_orders_by_department.html', orders_by_department=orders_by_department)


@app.route("/neo4j/product/timeofday")
def neo4j_quantity_by_time_of_day():
    quantity_by_time_of_day_results = Neo4jInstacart().HourWiseQuantity()
    return render_template('neo4j_products_by_time_of_day.html',
                           quantity_by_time_of_day=quantity_by_time_of_day_results)


@app.route('/neo4j/department/users')
def neo4j_max_orders_by_user():
    max_orders_by_user_result = Neo4jInstacart().UserOrder()
    return render_template('neo4j_max_orders_by_user.html', max_orders_by_user=max_orders_by_user_result)


@app.route("/neo4j/product/dayofweek")
def neo4j_products_by_day_of_week():
    products_by_day_of_week_results = Neo4jInstacart().DayWiseQuantity()
    return render_template('neo4j_products_by_day_of_week.html',
                           products_by_day_of_week=products_by_day_of_week_results)


class Neo4jInstacart:
    def __init__(self):
        try:
            self.graph = Graph("localhost:7474", username="instacart", password="Atma12#$")
            self.node_matcher = NodeMatcher(self.graph)
            self.rel_matcher = RelationshipMatcher(self.graph)
            print("Connection Successful", self.graph)
            # return graph
        except:
            print("An exception occurred")
            raise

    # Add try .. except for functions
    # def AisleProductQuantity(graph):
    def AisleProductQuantity(self):
        # result1=graph.run("MATCH (p:Product)-[r:ON]->(a:Aisle) RETURN a.name, count(r) AS products_on_aisle ORDER
        # BY products_on_aisle DESC LIMIT 20").data() result1=self.graph.run("MATCH (p:Product)-[r:ON]->(a:Aisle)
        # RETURN a.name, count(r) AS products_on_aisle ORDER BY products_on_aisle DESC LIMIT 20").data()
        result1 = self.graph.run(
            "MATCH (p:Product)-[r:ON]->(a:Aisle) RETURN a.name AS Aisle, count(r) AS Quantity ORDER BY Quantity DESC LIMIT 20").data()
        print(result1)
        return result1

    def DepartmentProductQuantity(self):
        result2 = self.graph.run(
            "MATCH (p:Product)-[r1:IN]->(d:Department),(p)-[r2:IN_ORDER]->(c:Order) RETURN d.name as department_name,count(r2) AS Number_of_Orders ORDER BY Number_of_Orders DESC").data()

        # result2=self.graph.run("MATCH (p:Product)-[r:IN]->(d:Department) RETURN d.name as department_name,count(r) AS product_count ORDER BY product_count DESC").data()
        print(result2)
        return result2

    def HourWiseQuantity(self):
        result3 = self.graph.run(
            "MATCH(o:Order) RETURN o.hourOfDay as TimeOfDayInHours, COUNT(o.hourOfDay) as salesAtHour  ORDER BY salesAtHour DESC").data()
        print(result3)
        return result3

    def UserOrder(self):
        result4 = self.graph.run(
            "MATCH (u:User)-[r1:ORDERED]->(o:Order),(p:Product)-[r2:IN_ORDER]->(o:Order) RETURN u.id as User_id,count(r2) AS Quantity ORDER BY Quantity DESC LIMIT 10").data()
        print(result4)
        return result4

    # CHANGE BELOW QUERY

    def DayWiseQuantity(self):
        result5 = self.graph.run(
            "MATCH (a:Aisle)<-[:ON]-(:Product)-[:IN]->(d:Department) WHERE (a.name CONTAINS 'meat' OR a.name CONTAINS 'seafood' OR d.name CONTAINS 'meat' OR a.name CONTAINS 'jerky') AND NOT a.name CONTAINS 'alternatives' AND NOT a.name CONTAINS 'marinades' with collect(DISTINCT a.name) AS avoidAisles MATCH (u:User) WITH u, avoidAisles limit 1000 match (u)-[:ORDERED]->(:Order)<-[:IN_ORDER]-(:Product)-[:ON]->(a:Aisle) WITH u, collect(DISTINCT a.name) AS aisles, avoidAisles WHERE none(a IN aisles WHERE a IN avoidAisles) WITH u MATCH (u)-[:ORDERED]->(:Order)<-[:IN_ORDER]-(p:Product) RETURN p.name, count(*) AS overlap ORDER BY overlap DESC").data()
        print(result5)
        return result5

    '''
    def TopOrderedProduct(graph):
        result2=graph.run("MATCH (p:Product)-[r:IN_ORDER]->() RETURN p.name, count(r) AS orderCount ORDER BY orderCount DESC LIMIT 20").data()
        print(result2)

    def SalesEveryHour(graph):
        result3=graph.run("MATCH(o:Order) RETURN o.hourOfDay as hourOfDay, COUNT(o.hourOfDay) as salesAtHour  ORDER BY salesAtHour DESC").data()
        print(result3)
    '''


if __name__ == '__main__':
    app.run()
