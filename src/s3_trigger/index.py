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

import uuid
import boto3
import os

MODEL_NAME = os.environ['MODEL_NAME']


def handler(event, context):

    print(event)

    sm_client = boto3.client('sagemaker')

    records = event['Records']

    for record in records:

        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        batch_job_name = str(uuid.uuid4())
        batch_input = 's3://{}/{}'.format(bucket, key)
        output_key = "result/" + "/".join(key.split("/")[1:-1])
        batch_output = 's3://{}/{}'.format(bucket, output_key)
        model_name = MODEL_NAME

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
