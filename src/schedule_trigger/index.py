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
import datetime
import uuid
import boto3

MODEL_NAME = os.environ['MODEL_NAME']

SCHEDULE_RATE = os.environ['SCHEDULE_RATE']
S3_BUCKET = os.environ['S3_BUCKET']


def handler(event, context):

    print(event)

    data_path = get_data_path()
    bucket = S3_BUCKET
    key = data_path
    batch_job_name = str(uuid.uuid4())
    batch_input = 's3://{}/{}'.format(bucket, key)
    output_key = "result/" + "/".join(key.split("/")[1:-1])
    batch_output = 's3://{}/{}'.format(bucket, output_key)
    model_name = MODEL_NAME

    s3_client = boto3.resource('s3')
    s3_bucket = s3_client.Bucket(S3_BUCKET)
    objects_size = sum(1 for _ in s3_bucket.objects.filter(Prefix=data_path))

    if objects_size > 0:

        sm_client = boto3.client('sagemaker')

        request = {
            "TransformJobName": batch_job_name,
            "ModelName": model_name,
            "BatchStrategy": "MultiRecord",
            "DataProcessing": {
                "InputFilter": "$[2:]",
                "JoinSource": "Input",
                "OutputFilter": "$[0,1,-1]"
            },
            "TransformOutput": {
                "S3OutputPath": batch_output,
                "Accept": "text/csv",
                "AssembleWith": "Line",
            },
            "TransformInput": {
                "DataSource": {
                    "S3DataSource": {
                        "S3DataType": "S3Prefix",
                        "S3Uri": batch_input
                    }
                },
                "ContentType": "text/csv",
                "SplitType": "Line",
                "CompressionType": "None"
            },
            "TransformResources": {
                    "InstanceType": "ml.m4.xlarge",
                    "InstanceCount": 1
            }
        }

        sm_response = sm_client.create_transform_job(**request)

        print(sm_response)
    else:

        print("No object available for processing.")


def get_data_path():

    if SCHEDULE_RATE == 'minute':
        previous_date = datetime.datetime.today() - datetime.timedelta(minutes=1)
        data_path = "data/{}/{}/{}/{}/{}/".format(previous_date.year,
                                                  previous_date.month,
                                                  previous_date.day,
                                                  previous_date.hour,
                                                  previous_date.minute)
    elif SCHEDULE_RATE == 'hour':
        previous_date = datetime.datetime.today() - datetime.timedelta(hours=1)
        data_path = "data/{}/{}/{}/{}/".format(
                                               previous_date.year,
                                               previous_date.month,
                                               previous_date.day,
                                               previous_date.hour)
    elif SCHEDULE_RATE == 'day':
        previous_date = datetime.datetime.today() - datetime.timedelta(days=1)
        data_path = "data/{}/{}/{}/".format(previous_date.year, previous_date.month, previous_date.day)
    elif SCHEDULE_RATE == 'month':
        last_month = datetime.date.today().replace(day=1) - datetime.timedelta(days=1)
        year, month = map(int, last_month.strftime("%Y %m").split())
        data_path = "data/{}/{}/".format(year, month)
    elif SCHEDULE_RATE == 'year':
        year = datetime.datetime.today().year - 1
        data_path = "data/{}/".format(year)

    return data_path
