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

dispos = s3.get_object(Bucket=BUCKET_NAME, Key='DUMP/Dispo1.csv')

dispos = pd.read_csv(
    dispos['Body'],
    # './Imports/Dispo1.csv',
    index_col=False,
    low_memory=False,
    encoding="ISO-8859-1",
)

users = s3.get_object(Bucket=BUCKET_NAME, Key='DUMP/Users1.csv')

users = pd.read_csv(
    users['Body'],
    # './Imports/Users1.csv',
    index_col=False,
    low_memory=False,
    encoding="ISO-8859-1",
)

dispos['cleanDate'] = pd.to_datetime(
  dispos['date'],
  format='%Y%m%d',
  utc=True)

# Make dispo lower case
dispos['dispo'] = dispos['dispo'].str.lower()

# Drop admin dispos
dispos = dispos[dispos["dispo"].str.contains('filepayupdate|fileupdate|closer|dupe|fixerror|oth|callasap|unreserve|2ndfr|undoholdsale|rshsale|spam|undocancel|qarecover|undopendingcancel|vonurt|taskdone|temp dnc|error|pendingcancel|frchg|holdsale|vocbk|voblowout|votask|attyupdate|attyvo')==False]
dispos = dispos.loc[-dispos['dispo'].isin([False])]

# Grab the newest dispo for each deal 
dispos = dispos.sort_values('cleanDate').groupby('dealid').tail(1)

# Get the employee's first name
dispos = pd.merge(dispos, users, how='left',
        left_on='userid', right_on='userid')

dispos['fname'] = dispos['fname'].apply(rr_fun.fronter_name)
dispos['lname'] = dispos['lname'].apply(rr_fun.fronter_name)

# Only need a couple columns
dispos = dispos[['cleanDate', ' dealid', 'userid', 'fname', 'lname']]
dispos.rename(columns={'fname': 'Fronter First', 'lname': 'Fronter Last'}, inplace=True)

dispos.to_csv('./Exports/finaldispos.csv')