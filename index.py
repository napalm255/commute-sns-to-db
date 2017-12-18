"""Commute SNS to Database."""

import sys
import logging
import os
import json
from collections import OrderedDict
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
    # sys.exit()


def handler(event, context):
    """Lambda handler."""
    # pylint: disable=unused-argument
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.info(event)

    header = {'Content-Type': 'application/json'}
    edata = event['Records'][0]['Sns']['Message']

    table_name = 'traffic'
    table_data = OrderedDict([
        ('id', {'type': 'INT NOT NULL AUTO_INCREMENT',
                'value': ''}),
        ('timestamp', {'type': 'TIMESTAMP NOT NULL',
                       'value': edata['timestamp']}),
        ('origin', {'type': 'VARCHAR(255) NOT NULL',
                    'value': edata['origin']}),
        ('destination', {'type': 'VARCHAR(255) NOT NULL',
                         'value': edata['destination']}),
        ('distance_miles', {'type': 'FLOAT',
                            'value': edata['distance']['text'].replace(' mi', '')}),
        ('distance_meters', {'type': 'FLOAT',
                             'value': edata['distance']['value']}),
        ('duration', {'type': 'INT',
                      'value': edata['duration']['value']}),
        ('duration_in_traffic', {'type': 'INT',
                                 'value': edata['duration_in_traffic']['value']})
    ])
    table_scheme = ', '.join([x + " " + y['type'] for x, y in table_data.iteritems()])
    columns = ', '.join(table_data.keys())
    values = ', '.join(['"' + str(y['value']) + '"' for x, y in table_data.iteritems() if
                        'id' not in x])

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
        cursor.execute('INSERT INTO traffic (%s) VALUES (%s)' % (columns, values))
        connection.commit()

    message = {'databases': databases,
               'database': {
                   'name': DATA['db_name'],
                   'tables': tables}}

    return {'statusCode': 200,
            'body': json.dumps(message),
            'headers': header}

if __name__ == '__main__':
    handler({'foo': 'bar'}, None)
