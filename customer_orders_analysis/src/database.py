from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Sequence
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class FactTable(Base):
    __tablename__ = "fact_table"

    id = Column(Integer, Sequence('id'), primary_key=True)
    order_id = Column(String)
    customer_id = Column(String)
    order_status = Column(String)
    order_purchase_timestamp = Column(DateTime)
    order_approved_at = Column(DateTime)
    order_delivered_carrier_date = Column(DateTime)
    order_delivered_customer_date = Column(DateTime)
    order_estimated_delivery_date = Column(DateTime)
    customer_unique_id = Column(String)
    customer_zip_code_prefix = Column(Integer)
    customer_city = Column(String)
    customer_state = Column(String)
    order_item_id = Column(Integer)
    product_id = Column(String)
    seller_id = Column(String)
    shipping_limit_date = Column(DateTime)
    price = Column(Float)
    freight_value = Column(Float)
    payment_sequential = Column(Integer)
    payment_type = Column(String)
    payment_installments = Column(Integer)
    payment_value = Column(Float)
    product_category_name = Column(String)
    seller_zip_code_prefix = Column(Integer)
    seller_city = Column(String)
    seller_state = Column(String)
    product_category_id = Column(Integer)
    product_category_name_english = Column(String)


class Top_Selling_Sellers(Base):
    __tablename__ = "top_selling_sellers"

    id = Column(Integer, Sequence('id'), primary_key=True)
    seller_id = Column(String)
    Total_sales = Column(Float)

class Most_Used_Payment_Type(Base):
    __tablename__ = "most_used_payment_type"

    id = Column(Integer, Sequence('user_id_seq'), primary_key=True)
    payment_type = Column(String)
    Count = Column(Integer)

class Top_Selling_Product_Category(Base):
    __tablename__ = "top_selling_product_category"

    id = Column(Integer, Sequence('id'), primary_key=True)
    product_category_name_english = Column(String)
    Total_sales = Column(Float)

class Order_Status_Count(Base):
    __tablename__ = "order_status_count"

    id = Column(Integer, Sequence('id'), primary_key=True)
    order_status = Column(String)
    Count = Column(Integer)

class Top_Average_Delivery_Time(Base):
    __tablename__ = "top_average_delivery_time"

    id = Column(Integer, Sequence('id'), primary_key=True)
    product_category_name_english = Column(String)
    Average_delivery_duration_days = Column(Float)

class Average_Payment_Value(Base):
    __tablename__ = "average_payment_value"

    id = Column(Integer, Sequence('id'), primary_key=True)
    payment_type = Column(String)
    Average_payment_value = Column(Float)

class Loyal_Customers(Base):
    __tablename__ = "loyal_customers"

    id = Column(Integer, Sequence('id'), primary_key=True)
    customer_unique_id = Column(String)
    no_of_orders = Column(Integer)


