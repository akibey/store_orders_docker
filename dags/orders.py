import os.path

from airflow.decorators import dag
# from airflow.providers.postgres.operators.postgres import PostgresOperator, PostgresHook

import pendulum
# import requests
# import json
# import pandas as pd
# import random
# from numpy.random import randint
# import psycopg2
# from sqlalchemy import create_engine
# import numpy as np
from create_db import create_database
from create_tables import create_aisles, create_departments, create_products, create_orders, create_orders_products
from get_orders import get_orders
from insert_data import add_aisles, add_departments, add_orders, add_products, add_orders_products

@dag(
    dag_id='orders',
    schedule_interval="@daily",
    start_date=pendulum.datetime(2022, 6, 10),
    catchup=False,
)
def orders():
    # create database
    order_db = create_database()
    # create tables :
    # 1st parallel block of create tables
    aisles = create_aisles()
    departments = create_departments()
    orders = create_orders()
    # create products table
    products = create_products()
    # create orders_products table
    orders_products = create_orders_products()

    # get the batch of orders and add to inp_orders table
    request_orders = get_orders()

    # fill tables:
    # aisles, products and orders tables can be populated in parallel
    addaisle = add_aisles()
    adddept = add_departments()
    add_order = add_orders()
    #
    add_prod = add_products()
    add_order_prod = add_orders_products()
    # order_db.set_downstream([aisles, departments])
    order_db >> [aisles, departments, orders] >> products >> orders_products
    orders_products.set_downstream(request_orders)
    request_orders >> [addaisle, adddept, add_order] >> add_prod >> add_order_prod

fin_orders = orders()
