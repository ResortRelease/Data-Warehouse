import secret
import re
import logging
from io import StringIO
from datetime import datetime
import pandas as pd
import numpy as np
from progress.bar import Bar

import boto3 #AWS
import botocore

import rr_fun

s3 = boto3.client('s3')
BUCKET_NAME = 'fabiano-crm-consys'

contacts = s3.get_object(Bucket=BUCKET_NAME, Key='DUMP/Deals1.csv')

contacts = pd.read_csv(
    contacts['Body'],
    index_col=0,
    low_memory=False,
    encoding="ISO-8859-1",
)

airtime = pd.read_csv(
    './airtime.csv',
    index_col=0,
    low_memory=False,
    encoding="ISO-8859-1",
)

# Remove junk from caller number (-)
airtime['Caller Number'] = airtime['Caller Number'].apply(rr_fun.clean_number)

# Make a list of unique phone numbers to iterate.
unique_phone = airtime['Caller Number'].unique().tolist()

# Remove junk from phone number
contacts['HomePhone'] = contacts['HomePhone'].apply(rr_fun.clean_number)

# Only return contacts
contacts = contacts.loc[contacts['HomePhone'].isin(unique_phone)]

contacts.to_csv('export-test.csv')


unique_phone = contacts['HomePhone'].unique()
# Only return contacts
contacts = contacts.loc[contacts['HomePhone'].isin(unique_phone)]
contacts.to_csv("unique-phone.csv")