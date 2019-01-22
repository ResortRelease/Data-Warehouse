import pandas as pd
import numpy as np
from datetime import datetime

# General functions for RR
import rr_fun

import boto3 #AWS
import botocore

s3 = boto3.client('s3')
BUCKET_NAME = 'fabiano-crm-consys'

webhook = s3.get_object(Bucket=BUCKET_NAME, Key='DUMP/WebHook1.csv')

webhook = pd.read_csv(
    webhook['Body'],
    # './Imports/Dispo1.csv',
    index_col=False,
    low_memory=False,
    encoding="ISO-8859-1",
)

list_webhook = webhook.UTMAdGroup.unique()
list_webhook = pd.Series(list_webhook)
list_webhook.to_csv('file.csv')