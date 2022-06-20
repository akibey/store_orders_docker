from airflow.decorators import task

import psycopg2


@task()
def add_aisles():
    conn = psycopg2.connect(
        user='airflow', database="orders",
        password='airflow', host='postgres'
    )
    conn.autocommit = True
    cur = conn.cursor()
    sql = """
    INSERT INTO aisles(aisle) 
    SELECT aisle from inp_orders
    ON CONFLICT (aisle) DO NOTHING; 
    """
    cur.execute(sql)
    conn.close()


@task()
def add_departments():
    conn = psycopg2.connect(
        user='airflow', database="orders",
        password='airflow', host='postgres'
    )
    conn.autocommit = True
    cur = conn.cursor()
    sql = """
    INSERT INTO departments(department) 
    SELECT  department from inp_orders
    ON CONFLICT (department) DO NOTHING; 
    """
    cur.execute(sql)
    conn.close()


@task()
def add_orders():
    conn = psycopg2.connect(
        user='airflow', database="orders",
        password='airflow', host='postgres'
    )
    conn.autocommit = True
    cur = conn.cursor()
    sql = """
    INSERT INTO orders(user_id, order_number, order_dow, 
    order_hr_of_day, days_since_prior_order) 
    SELECT user_id, order_number, order_dow, 
    order_hour_of_day, days_since_prior_order from inp_orders
    ON CONFLICT (order_number) DO NOTHING; 
    """
    cur.execute(sql)
    conn.close()

@task()
def add_products():
    conn = psycopg2.connect(
        user='airflow', database="orders",
        password='airflow', host='postgres'
    )
    conn.autocommit = True
    cur = conn.cursor()
    sql = """
    WITH inputvalues(product, aisle, department) AS (
    SELECT product, aisle, department FROM inp_orders 
    )
    INSERT INTO products (product, aisle_id, dept_id)
    SELECT d.product, ai.id, dep.id
    FROM inputvalues AS d
    INNER JOIN departments AS dep 
    ON d.department = dep.department 
    INNER JOIN aisles AS ai
    ON d.aisle = ai.aisle  ON conflict (product) do nothing;
    """
    cur.execute(sql)
    conn.close()


@task()
def add_orders_products():
    conn = psycopg2.connect(
        user='airflow', database="orders",
        password='airflow', host='postgres'
    )
    conn.autocommit = True
    cur = conn.cursor()
    sql = """
    WITH inputvalues(order_number, product, add_to_cart_order ,reordered ) AS (
    SELECT order_number, product, add_to_cart_order, reordered FROM inp_orders 
    )
    INSERT INTO orders_products (order_id, product_id, add_to_cart_order, reordered)
    SELECT o.id, p.id, inp.add_to_cart_order, inp.reordered
    FROM inputvalues AS inp
    INNER JOIN products AS p 
    ON inp.product = p.product 
    INNER JOIN orders AS o
    ON inp.order_number = o.order_number;
    """
    cur.execute(sql)
    conn.close()