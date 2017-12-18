"""Commute SNS to Database."""
import json
import datetime
import pymysql


def handler(event, context):
    """Lambda handler."""
    # pylint: disable=unused-argument
    data = {
        'output': 'Hello World',
        'timestamp': datetime.datetime.utcnow().isoformat(),
        'pymysql': str(pymysql)
    }
    return {'statusCode': 200,
            'body': json.dumps(data),
            'headers': {'Content-Type': 'application/json'}}
