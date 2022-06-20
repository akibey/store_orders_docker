import requests
import pandas as pd
from airflow.decorators import task
from sqlalchemy import create_engine

URL = "http://host.docker.internal:80/orders"
# PSYCOPG2_PARAMS = {'user': 'airflow', 'database': "orders",
#         'password': 'airflow', 'host': 'postgres'}

CONN_STRING = 'postgresql://airflow:airflow@postgres/orders'
@task()
def get_orders():
    result = requests.get(URL)
    orders = pd.read_json(result.json()).transpose()
    orders.drop("eval_set", axis=1, inplace=True)

    db = create_engine(CONN_STRING)
    conn = db.connect()
    orders.to_sql('inp_orders', con=conn, if_exists='replace', index=False)
