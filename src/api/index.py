#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#  SPDX-License-Identifier: MIT-0
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy of this
#  software and associated documentation files (the "Software"), to deal in the Software
#  without restriction, including without limitation the rights to use, copy, modify,
#  merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
#  permit persons to whom the Software is furnished to do so.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
#  INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
#  PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
#  HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
#  OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
#  SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

import json
import boto3
import uuid
import os
import decimal

JOB_STREAM_NAME = os.environ['JOB_STREAM_NAME']
JOB_TABLE_NAME = os.environ['JOB_TABLE_NAME']


def handler(event, context):

    print(event)

    method = event['httpMethod']
    resource = event['resource']

    if method == 'POST' and resource == '/job':
        return submit_job(event, context)
    elif method == 'GET' and resource == "/job/{id}":
        return get_job(event, context)


def submit_job(event, context):

    job_id = str(uuid.uuid4())

    db_put_job({
        'jobId': job_id,
        'status': 'Queued'
    })

    kinesis_put_job(job_id, event['body'])

    return {
        "statusCode": 200,
        "body": json.dumps({"job_id": job_id})
    }


def get_job(event, context):

    class DecimalEncoder(json.JSONEncoder):
        def default(self, o):
            if isinstance(o, decimal.Decimal):
                if abs(o) % 1 > 0:
                    return float(o)
                else:
                    return int(o)
            return super(DecimalEncoder, self).default(o)

    job_id = event['pathParameters']['id']

    job_item = db_get_job(job_id)

    return {
        "statusCode": 200,
        "body": json.dumps(job_item, indent=4, cls=DecimalEncoder)
    }


def db_put_job(item):

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(JOB_TABLE_NAME)
    table.put_item(
        Item=item
    )


def kinesis_put_job(job_id, job_body):

    kinesis_client = boto3.client('kinesis')

    kinesis_client.put_record(
        StreamName=JOB_STREAM_NAME,
        Data=job_body,
        PartitionKey=job_id
    )


def db_get_job(job_id):

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(JOB_TABLE_NAME)
    response = table.get_item(
        Key={
          "jobId": job_id
        }
    )

    return response['Item']
