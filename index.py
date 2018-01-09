#!/usr/bin/env python
"""Commute SNS to Database."""
# pylint: disable=broad-except

from __future__ import print_function
import sys
import logging
import json
from collections import OrderedDict
import pymysql
from pymysql.cursors import DictCursor
import boto3


# logging configuration
logging.getLogger().setLevel(logging.INFO)

try:
    SSM = boto3.client('ssm')

    PREFIX = '/commute/database'
    PARAMS = SSM.get_parameters_by_path(Path=PREFIX, Recursive=True,
                                        WithDecryption=True)
    logging.debug('ssm: parameters(%s)', PARAMS)

    DATABASE = dict()
    for param in PARAMS['Parameters']:
        key = param['Name'].replace('%s/' % PREFIX, '')
        DATABASE.update({key: param['Value']})
    logging.debug('ssm: database(%s)', DATABASE)

    logging.info('ssm: successfully gathered parameters')
except Exception as ex:
    logging.error('ssm: could not connect to SSM. (%s)', ex)
    sys.exit()


try:
    CONNECTION = pymysql.connect(host=DATABASE['host'],
                                 user=DATABASE['user'],
                                 password=DATABASE['pass'],
                                 autocommit=True,
                                 cursorclass=DictCursor)
    logging.info('database: successfully connected to mysql')
# pylint: disable=broad-except
except Exception as ex:
    logging.error('database: could not connect to mysql (%s)', ex)
    sys.exit()

try:
    CLOUDWATCH = boto3.client('cloudwatch')
# pylint: disable=broad-except
except Exception as ex:
    logging.error('cloudwatch: could not connect to cloudwatch (%s)', ex)
    sys.exit()


def publish_metric(value, timestamp):
    """Publish CloudWatch Metric."""
    response = CLOUDWATCH.put_metric_data(
        Namespace='commute',
        MetricData=[
            {
                'MetricName': 'duration_in_traffic',
                'Timestamp': timestamp,
                'Value': value,
                'Unit': 'Seconds'
            }
        ]
    )
    return response


def handler(event, context):
    """Lambda handler."""
    # pylint: disable=unused-argument, too-many-locals
    logging.info(event)

    header = {'Content-Type': 'application/json'}
    edata = json.loads(event['Records'][0]['Sns']['Message'])

    table_name = 'traffic'
    logging.info('table data: setup')
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
    logging.debug('table data: %s', table_data)
    logging.info('table data: finished')
    table_scheme = [x + " " + y['type'] for x, y in table_data.items()]
    columns = list(table_data.keys())[1:]
    values = [str(y['value']) for x, y in table_data.items() if
              'id' not in x]
    tbl = ', '.join(table_scheme)
    cols = ', '.join(columns)
    vals = ', '.join(values)

    logging.info('database: start')
    with CONNECTION.cursor() as cursor:
        # check if database exists
        cursor.execute('show databases')
        databases = cursor.fetchall()
        logging.debug('databases: %s', databases)
        # create database if it does not exist
        if {'Database': DATABASE['name']} not in databases:
            sql = 'CREATE DATABASE %s' % (DATABASE['name'])
            logging.info('database: creating (%s)', sql)
            cursor.execute(sql)
            logging.info('database: created')
        # use the database
        CONNECTION.select_db(DATABASE['name'])
        # check if table exists
        cursor.execute('show tables')
        tables = cursor.fetchall()
        logging.debug('tables: %s', tables)
        # create table if it does not exist
        if {'Tables_in_commute': table_name} not in tables:
            sql = 'CREATE TABLE %s (%s, PRIMARY KEY (id))' % (table_name, tbl)
            logging.info('table: creating (%s)', sql)
            cursor.execute(sql)
            logging.info('table: created')
        # insert new record
        logging.info('record: inserting')
        sql = 'INSERT INTO %s (%s) VALUES (%s)' % (table_name, cols, vals)
        logging.debug('record: %s', sql)
        cursor.execute(sql)
        logging.info('record: inserted')
    logging.info('database: end')

    # publish cloudwatch metric
    try:
        publish_metric(edata['duration_in_traffic']['value'], edata['timestamp'])
    except Exception:
        pass

    return {'statusCode': 200,
            'body': json.dumps({'status': 'OK'}),
            'headers': header}


if __name__ == '__main__':
    with open('test.json') as json_file:
        print(handler(json.load(json_file), None))
