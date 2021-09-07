from airflow import DAG
from datetime import datetime, timedelta

from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

import os
import json
import urllib.request
import re

default_args = {
    'owner': 'airflow',
}

with DAG(
    dag_id = 'json_etl_dag',
    description = 'ETL DAG Challenge',
    default_args = default_args,
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
) as dag:

    def extract_data(**kwargs):
        ti = kwargs['ti']

        users_endpoint = 'https://jsonplaceholder.typicode.com/users'
        posts_endpoint = 'https://jsonplaceholder.typicode.com/posts'
        with urllib.request.urlopen(users_endpoint) as url:
            users_data = json.loads(url.read().decode())
        with urllib.request.urlopen(posts_endpoint) as url:
            posts_data = json.loads(url.read().decode())

        dir_name = os.path.dirname(__file__)
        users_path = os.path.join(dir_name, "files/users")
        posts_path = os.path.join(dir_name, "files/posts")

        with open(users_path, 'w') as open_users:
            for user in users_data:
                id = user['id']
                name= user['name']
                username= user['username']
                email= user['email']
                address_street = user['address']['street']
                address_suite = user['address']['suite']
                address_city = user['address']['city']
                address_zipcode = user['address']['zipcode']
                address_geo_lat = user['address']['geo']['lat']
                address_geo_lng = user['address']['geo']['lng']
                phone = user['phone']
                website = user['website']
                company_name = user['company']['name']
                company_catchPhrase = user['company']['catchPhrase']
                company_bs = user['company']['bs']
                
                phone = standardize_phone(phone)

                # Do vertical bars because I think they're easier
                open_users.write("%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s\n" 
                                 % (id, name, username, email, address_street, address_suite,
                                 address_city, address_zipcode, address_geo_lat,
                                 address_geo_lng, phone, website, company_name, 
                                 company_catchPhrase, company_bs))

        with open(posts_path, 'w') as open_posts:
            for post in posts_data:
                userId = post['userId']
                id = post['id']
                title = post['title']
                body = post['body']

                # Make sure stuff that might cause problems are escaped
                title = insert_escapes(title)
                body = insert_escapes(body)

                # Definitely vertical bars for text even if we can escape them
                open_posts.write("%s|%s|%s|%s\n" % (id, userId, title, body))

        # Make sure we pass where we put our users and posts files to the next function
        ti.xcom_push(key='users_path', value=users_path)
        ti.xcom_push(key='posts_path', value=posts_path)


    def load_data(**kwargs):
        ti = kwargs['ti']
        pg_hook = PostgresHook(postgres_conn_id='nickel-aws')

        # Retrieve the locations of the files
        users_path = ti.xcom_pull(task_ids='extract_data', key= 'users_path')
        posts_path = ti.xcom_pull(task_ids='extract_data', key= 'posts_path')

        # Execute the copy_expert command using the PostgresHook
        execute_load(pg_hook, "users", users_path)
        execute_load(pg_hook, "posts", posts_path)

    # Just makes it look less ugly when we want to call it on both sources of data
    def execute_load(pg_hook, table, file_path):
        conn = pg_hook.get_conn()

        import_query = """
        COPY %s_temp FROM STDIN (delimiter '|')
        """ % (table)
        pg_hook.copy_expert(import_query, file_path)

    # We assume for this data set there's only US people so we don't need the 1
    # in front of the phone number. This essentially removes all formatting for
    # phone numbers except for x0000 for extensions.
    # Example input vs output: 1-770-736-8031 x56442 -> 7707368031x56442
    def standardize_phone(phone):
        transform = ''.join(re.findall(r"(x?[0-9]+)+", phone))
        ext = ''.join(re.findall(r"(x[0-9]+)",transform))
        phone_no_ext = re.findall(r"([0-9]+){1}", transform)[0]
        if len(phone_no_ext) == 11 and phone_no_ext[0] == '1':
            phone_no_ext = phone_no_ext[1:]
        return phone_no_ext + ext

    # Make sure we escape text that might mess importing into our database.
    def insert_escapes(text):
        return re.escape(text)

    # Here we obtain the data from our endpoints and store them in files in a format
    # that's more easily readable for the database import.
    extract = PythonOperator(
        task_id='extract_data',
        python_callable = extract_data,
    )

    # Make sure all the tables we need are created. Turns out temp tables don't persist
    # between different PostgresOperators since it's a new connection, so we're using
    # actual tables for now. Also make sure to drop the previous "temp" tables in case
    # there was an error and they now contain data.
    create_tables = PostgresOperator(
        task_id = 'create_tables',
        postgres_conn_id = 'nickel-aws',
        sql = """
            DROP TABLE IF EXISTS users_temp;
            DROP TABLE IF EXISTS posts_temp;
            CREATE TABLE IF NOT EXISTS posts (id INT PRIMARY KEY, userId INT, title TEXT, body TEXT);
            CREATE TABLE IF NOT EXISTS users (id INT PRIMARY KEY, name TEXT, username TEXT, email TEXT, address_street TEXT, address_suite TEXT, address_city TEXT, address_zipcode TEXT, address_geo_lat numeric, address_geo_lng numeric, phone TEXT, website TEXT, company_name TEXT, company_catchPhrase TEXT, company_bs TEXT);
            CREATE TABLE IF NOT EXISTS users_temp AS SELECT * FROM users WHERE 1=0;
            CREATE TABLE IF NOT EXISTS posts_temp AS SELECT * FROM posts WHERE 1=0;
        """
    )

    # Take the data from our files and upload them to the database using a PostgresHook
    load = PythonOperator(
        task_id = 'load_data',
        python_callable = load_data,
    )

    # Finish up the load by making sure the information in the "temp" tables are moved into the end tables. We're doing it this way so we can make sure we don't have duplicate data.
    finish_load = PostgresOperator(
        task_id = 'finish_load',
        postgres_conn_id = 'nickel-aws',
        sql = """
            INSERT INTO users SELECT * FROM users_temp a WHERE NOT EXISTS (SELECT * FROM users b where a.id = b.id);
            INSERT INTO posts SELECT * FROM posts_temp a WHERE NOT EXISTS (SELECT * FROM posts b where a.id = b.id);
            DROP TABLE IF EXISTS users_temp;
            DROP TABLE IF EXISTS posts_temp;
        """
    )

    extract >> create_tables >> load >> finish_load