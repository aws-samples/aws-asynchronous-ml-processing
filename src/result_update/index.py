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

import os
import boto3
import time

JOB_TABLE_NAME = os.environ['JOB_TABLE_NAME']


def handler(event, context):

    print(event)

    s3 = boto3.resource('s3')
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(JOB_TABLE_NAME)

    records = event['Records']

    for record in records:

        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']

        s3_obj = s3.Object(bucket, key)
        s3_content = s3_obj.get()['Body'].read().decode('utf-8')

        results = s3_content.splitlines()

        with table.batch_writer() as batch:

            for result in results:

                result_arr = result.split(",")
                updated_item = {
                    "jobId": result_arr[0],
                    "arrivalTime": result_arr[1],
                    "processedTime": str(time.time()),  # Floats are not supported in DynamoDB
                    "result": result_arr[2],
                    "status": "Processed"
                }

                batch.put_item(Item=updated_item)
