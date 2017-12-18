"""Commute SNS to Database."""

import logging
import json
import datetime
import pymysql


def handler(event, context):
    """Lambda handler."""
    # pylint: disable=unused-argument
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.info(event)

    header = {'Content-Type': 'application/json'}

    data = {
        'timestamp': datetime.datetime.utcnow().isoformat(),
        'pymysql': str(pymysql)
    }
    logging.info(data)

    return {'statusCode': 200,
            'body': json.dumps(data),
            'headers': header}
