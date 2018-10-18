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

s3 = boto3.client('s3')
BUCKET_NAME = 'fabiano-crm-consys'

# What are we looking for? [email, phone]
# Which status should be included? Provide a list by commas.

bar = Bar('Processing', max=3)
bar.start()

##### Import Deals Sheet #####
# Get file from S3.
# contacts = s3.get_object(Bucket=BUCKET_NAME, Key='DUMP/Deals1.csv')

# # Load it into dataframe without saving
# contacts = pd.read_csv(
#     contacts['Body'],
#     index_col=0,
#     low_memory=False,
#     encoding="ISO-8859-1",
# )

contacts = pd.read_csv(
  './Deals1.csv',
  low_memory=False,
  index_col=0,
  encoding="ISO-8859-1",
)

# We only need a handful of columns
# contacts.drop(columns=['status', 'datecr', 'salesdate', 'ClientName', 'NameofResort', 'HomePhone', 'EmailAddress', 'lastdispo', 'hasform', 'dnc'])
contacts = contacts.drop(columns=['LeadSource', 'SubSource', 'timestamp', 'timeASAP', 'dateASAP', 'aweberid', 'salesfusionid', 'mortgage', 'appsetdate', 'appverdate', 'cancelsale', 'holdsale', 'sold_tr', 'sold_mt', 'sold_tr_rev', 'sold_mt_rev', 'FRONTER_REP', 'CLOSER_REP', 'VERIFY', 'StreetAddress', 'City', 'State', 'ZipCode', 'SecondaryPhone', 'timezone', 'reserve', 'reservetime', 'reserveuser', 'lastfronter', 'lastdispodate', 'emergency', 'utm_term', 'utm_campaign', 'utm_source', 'utm_medium', 'utm_content', 'hasapp', 'sold_tr_rev_net', 'sold_mt_rev_net', 'hearduson', 'heardother', 'sold_tr1', 'sold_mt1', 'tfdb_case', 'qareport', 'qareport1', 'MQL', 'SQL', 'SQT', 'SQT.1', 'Nurture'])

bar.next()

# Let only import numbers from phone columns
def clean_number(number):
  if pd.isna(number) == True:
    return 0
  else:
    return re.sub("\D", "", number)

# Function for readiblity
def clean_email(email):
  pattern = re.compile("(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)")
  if pd.isna(email) == False:
    email_exists = bool(pattern.match(email))
    if email_exists == True:
      email = email.lower()
      email = email.strip()
      return email
    else:
      return "Not a valid email"
  else:
    return "Not a valid email"

bar.next()

# Format the dates to MM/DD/YYYY
def format_date(date):
  if pd.isna(date) == False:
    string_date = str(date)
    month = string_date[4:6]
    day = string_date[6:8]
    year = string_date[0:4]
    return (f'{month}/{day}/{year}')

def clean_status(x):
  if pd.isna(x) == False:
    cleaned = re.findall("^[a-zA-Z09]*[^-]", x)[0]
    return cleaned

def clean_client_name(x):
  if pd.isna(x) == False:
    cleaned = re.findall("^[a-zA-Z0-9]*", x)[0]
    cleaned = cleaned.title()
    return cleaned

def clean_name(x):
  if pd.isna(x) == False:
    # We are going to get rid of everything after the 'and' + '&' so it looks like a name.
    return x.title()

# Clean phone number
contacts['HomePhone'] = contacts['HomePhone'].apply(clean_number)

# Clean emails
contacts['EmailAddress'] = contacts['EmailAddress'].apply(clean_email)
# Drop "Not a valid email"
contacts = contacts.loc[-contacts['EmailAddress'].isin(["Not a valid email"])]

# Clean created date
contacts['datecr'] = contacts['datecr'].apply(format_date)

# Clean sales date
contacts['salesdate'] = contacts['salesdate'].apply(format_date)

# Clean sales date
contacts['lastdispo'] = contacts['lastdispo'].apply(clean_status)

# Clean the names of clients and resorts
contacts['ClientName'] = contacts['ClientName'].apply(clean_client_name)
contacts['NameofResort'] = contacts['NameofResort'].apply(clean_name)

# Make columns string
contacts['hasform'] = contacts['hasform'].astype(str)
contacts['dnc'] = contacts['dnc'].astype(str)

# Merge duplicates
contacts = contacts.groupby('EmailAddress').agg({
                             'datecr': 'first', 
                             'salesdate': 'any', 
                             'ClientName': 'first', 
                             'NameofResort': 'first', 
                             'lastdispo': 'first', 
                             'hasform': ', '.join, 
                             'status': ', '.join, 
                             'dnc':', '.join})

# Drop dnc with 1 
contacts = contacts[contacts["dnc"].str.contains('1')==False]
contacts = contacts.loc[-contacts['dnc'].isin([False])]

# Drop if there is a sale date
contacts = contacts.loc[contacts['salesdate'].isin([False])]

# Drop if the last dispo is terminate
contacts = contacts[contacts["lastdispo"].str.contains('DNC|WRONG|GOTRID|AWC|CANT|NIT')==False]
contacts = contacts.loc[-contacts['lastdispo'].isin([False])]

# Optional: Filter down to users who have been contacted recently
contacts = contacts[contacts["hasform"].str.contains('nan')==False]
contacts = contacts.loc[-contacts['hasform'].isin([False])]

contacts = contacts[contacts["status"].str.contains('1|2')==True]
contacts = contacts.loc[-contacts['status'].isin([False])]

contacts.to_csv('export-test.csv')

bar.next()

bar.finish()