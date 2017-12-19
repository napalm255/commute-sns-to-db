#!/usr/bin/env python
"""Commute SNS to Database."""

from __future__ import print_function
import sys
import logging
import os
import json
from collections import OrderedDict
import pymysql
from pymysql.cursors import DictCursor


try:
    DATA = {'db_host': os.environ['DATABASE_HOST'],
            'db_user': os.environ['DATABASE_USER'],
            'db_pass': os.environ['DATABASE_PASS'],
            'db_name': os.environ['DATABASE_NAME']}
    CONNECTION = pymysql.connect(host=DATA['db_host'],
                                 user=DATA['db_user'],
                                 password=DATA['db_pass'],
                                 cursorclass=DictCursor)
    logging.info('Successfully connected to MySql.')
# pylint: disable=broad-except
except Exception as ex:
    logging.error('Unexpected error: could not connect to MySql. (%s)', ex)
    sys.exit()


def handler(event, context):
    """Lambda handler."""
    # pylint: disable=unused-argument, too-many-locals
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.info(event)

    header = {'Content-Type': 'application/json'}
    edata = json.loads(event['Records'][0]['Sns']['Message'])

    table_name = 'traffic'
    logging.info('setup table data')
    table_data = OrderedDict([
        ('id', {'type': 'INT NOT NULL AUTO_INCREMENT',
                'value': ''}),
        ('timestamp', {'type': 'TIMESTAMP NOT NULL',
                       'value': '"' + edata['timestamp'] + '"'}),
        ('origin', {'type': 'VARCHAR(255) NOT NULL',
                    'value': '"' + edata['origin'] + '"'}),
        ('destination', {'type': 'VARCHAR(255) NOT NULL',
                         'value': '"' + edata['destination'] + '"'}),
        ('distance_miles', {'type': 'FLOAT',
                            'value': float(edata['distance']['text'].replace(' mi', ''))}),
        ('distance_meters', {'type': 'FLOAT',
                             'value': float(edata['distance']['value'])}),
        ('duration', {'type': 'INT',
                      'value': int(edata['duration']['value'])}),
        ('duration_in_traffic', {'type': 'INT',
                                 'value': int(edata['duration_in_traffic']['value'])})
    ])
    logging.info('gathered table data')
    table_scheme = [x + " " + y['type'] for x, y in table_data.iteritems()]
    columns = table_data.keys()[1:]
    values = [str(y['value']) for x, y in table_data.iteritems() if
              'id' not in x]
    tbl = ', '.join(table_scheme)
    cols = ', '.join(columns)
    vals = ', '.join(values)

    logging.info('start database work')
    with CONNECTION.cursor() as cursor:
        # check if database exists
        cursor.execute('show databases')
        databases = cursor.fetchall()
        logging.info(databases)
        # create database if it does not exist
        if DATA['db_name'] not in databases:
            logging.info('creating database')
            sql = 'CREATE DATABASE %s' % (DATA['db_name'])
            logging.info(sql)
            cursor.execute(sql)
            CONNECTION.commit()
            logging.info('created database')
        # use the database
        CONNECTION.select_db(DATA['db_name'])
        # check if table exists
        cursor.execute('show tables')
        tables = cursor.fetchall()
        logging.info(tables)
        # create table if it does not exist
        if table_name not in tables:
            logging.info('creating table')
            sql = 'CREATE TABLE %s (%s, PRIMARY KEY (id))' % (table_name, tbl)
            cursor.execute(sql)
            CONNECTION.commit()
            logging.info('table created')
        # insert new record
        logging.info('inserting record')
        sql = 'INSERT INTO %s (%s) VALUES (%s)' % (table_name, cols, vals)
        logging.info(sql)
        cursor.execute(sql)
        CONNECTION.commit()
        logging.info('inserted record')
    logging.info('end database work')

    return {'statusCode': 200,
            'body': json.dumps({'status': 'OK'}),
            'headers': header}


if __name__ == '__main__':
    with open('test.json') as json_file:
        print(handler(json.load(json_file), None))
