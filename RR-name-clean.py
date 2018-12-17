import secret
import re
import logging
from io import StringIO
from datetime import datetime
import pandas as pd
import numpy as np
from progress.bar import Bar

# Since the database is generally unclean theses are a group of functions that are used repeatedly
import rr_fun

import boto3 #AWS
import botocore

s3 = boto3.client('s3')
BUCKET_NAME = 'fabiano-crm-consys'

contacts = s3.get_object(Bucket=BUCKET_NAME, Key='DUMP/Deals1.csv')

# Load it into dataframe without saving
contacts = pd.read_csv(
    contacts['Body'],
    index_col=False,
    low_memory=False,
    encoding="ISO-8859-1",
)

contacts['Full Name'] = contacts['ClientName'].apply(rr_fun.clean_client_name)
contacts['First Name'], contacts['Last Name'] = contacts['Full Name'].str.split(' ', 1).str

contacts = contacts[['dealid', 'Full Name', 'First Name', 'Last Name']]
contacts.to_csv('./Exports/export-names.csv')