from airflow.decorators import  task

import psycopg2


@task()
def create_aisles():
    conn = psycopg2.connect(
        user='airflow', database="orders",
        password='airflow', host='postgres'
    )
    conn.autocommit = True
    cur = conn.cursor()
    sql = r"""
    CREATE TABLE IF NOT EXISTS aisles (id SERIAL PRIMARY KEY, aisle VARCHAR UNIQUE);
    """
    cur.execute(sql)
    conn.close()


@task()
def create_departments():
    conn = psycopg2.connect(
        user='airflow', database="orders",
        password='airflow', host='postgres'
    )
    conn.autocommit = True
    cur = conn.cursor()
    sql = r"""
    CREATE TABLE IF NOT EXISTS departments (id SERIAL PRIMARY KEY, department VARCHAR UNIQUE);
    """
    cur.execute(sql)
    conn.close()


@task()
def create_products():
    conn = psycopg2.connect(
        user='airflow', database="orders",
        password='airflow', host='postgres'
    )
    conn.autocommit = True
    cur = conn.cursor()
    sql = r"""
    CREATE TABLE IF NOT EXISTS 
    products (id SERIAL PRIMARY KEY, 
    product varchar unique,
    aisle_id INT REFERENCES aisles(id) ON DELETE SET NULL, 
    dept_id INT REFERENCES departments(id) ON DELETE SET NULL);
    """
    cur.execute(sql)
    conn.close()


@task()
def create_orders():
    conn = psycopg2.connect(
        user='airflow', database="orders",
        password='airflow', host='postgres'
    )
    conn.autocommit = True
    cur = conn.cursor()
    sql = r"""
    CREATE TABLE IF NOT EXISTS 
    orders (id SERIAL PRIMARY KEY, 
    user_id INT, order_number INT UNIQUE NOT NULL, order_dow INT, order_hr_of_day INT,
    days_since_prior_order float);
    """
    cur.execute(sql)
    conn.close()


@task()
def create_orders_products():
    conn = psycopg2.connect(
        user='airflow', database="orders",
        password='airflow', host='postgres'
    )
    conn.autocommit = True
    cur = conn.cursor()
    sql = r"""
    CREATE TABLE IF NOT EXISTS 
    orders_products (order_id INT REFERENCES orders(id) , 
    product_id INT REFERENCES products(id) ,add_to_cart_order INT,reordered INT);
    """
    cur.execute(sql)
    conn.close()