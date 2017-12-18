"""Commute SNS to Database."""

import sys
import logging
import os
import json
import pymysql


try:
    DATA = {'db_host': os.environ['DATABASE_HOST'],
            'db_user': os.environ['DATABASE_USER'],
            'db_pass': os.environ['DATABASE_PASS'],
            'db_name': os.environ['DATABASE_NAME']}
    connection = pymysql.connect(host=DATA['db_host'],
                                 user=DATA['db_user'],
                                 password=DATA['db_pass'])
    logging.info('Successfully connected to MySql.')
except:
    logging.error('Unexpected error: could not connect to MySql.')
    sys.exit()


def handler(event, context):
    """Lambda handler."""
    # pylint: disable=unused-argument
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.info(event)

    header = {'Content-Type': 'application/json'}

    table_name = 'traffic'
    table_scheme = ("id INT NOT NULL AUTO_INCREMENT,"
                    "timestamp TIMESTAMP NOT NULL,"
                    "origin VARCHAR(255) NOT NULL,"
                    "destination VARCHAR(255) NOT NULL,"
                    "distance_miles FLOAT,"
                    "distance_meters FLOAT,"
                    "duration INT,"
                    "duration_in_traffic INT")

    with connection.cursor() as cursor:
        # check if database exists
        cursor.execute('show databases')
        databases = cursor.fetchall()
        logging.info(databases)
        # create database if it does not exist
        if (DATA['db_name'],) not in databases:
            cursor.execute('CREATE DATABASE %s' % DATA['db_name'])
            connection.commit()
        # use the database
        connection.select_db(DATA['db_name'])
        # check if table exists
        cursor.execute('show tables')
        tables = cursor.fetchall()
        logging.info(tables)
        # create table if it does not exist
        if (table_name,) not in tables:
            cursor.execute('CREATE TABLE commute (%s)' % table_scheme)
            connection.commit()
        # select existing records
        cursor.execute('SELECT * FROM commute')
        records = cursor.fetchall()
        logging.info(records)
        # insert new record

    message = {'databases': databases,
               'database': {
                   'name': DATA['db_name'],
                   'tables': tables}}

    return {'statusCode': 200,
            'body': json.dumps(message),
            'headers': header}
