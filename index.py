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
    logger.setLevel(logging.DEBUG)
    logger.info(event)

    header = {'Content-Type': 'application/json'}

    with connection.cursor() as cursor:
        cursor.execute('show databases')
        databases = cursor.fetchall()
        logging.info('databases')
        if DATA['db_name'] not in databases:
            cursor.execute('CREATE DATABASE %s' % DATA['db_name'])
            cursor.execute('USE %s' % DATA['db_name'])
            connection.commit()
        cursor.execute('show tables')
        tables = cursor.fetchall()
        logging.info(tables)

    message = {'databases': databases,
               'database': {
                   'name': DATA['db_name'],
                   'tables': tables}}

    return {'statusCode': 200,
            'body': json.dumps(message),
            'headers': header}
