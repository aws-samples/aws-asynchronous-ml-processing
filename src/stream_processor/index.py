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

import base64
import os
import time
import uuid
import boto3


S3_BUCKET = os.environ['S3_BUCKET']


def handler(event, context):

    print(event)

    batch_id = str(uuid.uuid4())
    year, month, day, hour, minute = map(int, time.strftime("%Y %m %d %H %M").split())
    s3_data_content = ""

    records = event['Records']

    for record in records:

        record_id = record['kinesis']['partitionKey']
        record_arrival = record['kinesis']['approximateArrivalTimestamp']
        record_data = base64.b64decode(record['kinesis']['data']).decode('utf-8')
        s3_data_content = s3_data_content + record_id + "," + str(record_arrival) + "," + str(record_data) + '\n'

    s3 = boto3.resource('s3')
    data_path = "data/{}/{}/{}/{}/{}/{}/data".format(year, month, day, hour, minute, batch_id)
    s3.Object(S3_BUCKET, data_path).put(Body=s3_data_content)
