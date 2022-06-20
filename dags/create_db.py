from airflow.decorators import task

import psycopg2

@task()
def create_database():
    conn = psycopg2.connect(
        user='airflow', database="airflow",
        password='airflow', host='postgres'
    )
    conn.autocommit = True
    cursor = conn.cursor()
    sql = '''
                   SELECT COUNT(*) FROM pg_database WHERE datname = 'orders';
                '''
    cursor.execute(sql)
    results = cursor.fetchall()
    conn.close()

    print(results[0][0])
    if results[0][0] < 1:
        try:
            conn = psycopg2.connect(
                user='airflow', database="airflow",
                password='airflow', host='postgres'
            )
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("CREATE DATABASE orders WITH OWNER airflow;")
        finally:
            if conn:
                conn.close()