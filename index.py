"""Commute SNS to Database."""

import logging
import os
import json
import pymysql


def handler(event, context):
    """Lambda handler."""
    # pylint: disable=unused-argument
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.info(event)

    header = {'Content-Type': 'application/json'}

    data = {'db_host': os.environ['DATABASE_HOST'],
            'db_user': os.environ['DATABASE_USER'],
            'db_pass': os.environ['DATABASE_PASS'],
            'db_name': os.environ['DATABASE_NAME']}
    logging.info(data)

    connection = pymysql.connect(host=data['db_host'],
                                 user=data['db_user'],
                                 password=data['db_pass'])
    results = connection.cursor().execute('show tables')

    message = {'tables': results}

    return {'statusCode': 200,
            'body': json.dumps(message),
            'headers': header}
