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

# #### Domo Config #####

# from pydomo import Domo

# from pydomo.datasets import DataSetRequest, Schema, Column, ColumnType, Policy
# from pydomo.datasets import PolicyFilter, FilterOperator, PolicyType, Sorting, UpdateMethod

# # Build an SDK configuration
# client_id = secret.domo_id
# client_secret = secret.domo_secret
# api_host = 'api.domo.com'

# # Configure the logger
# handler = logging.StreamHandler()
# handler.setLevel(logging.INFO)
# formatter = logging.Formatter(
#     '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# handler.setFormatter(formatter)
# logging.getLogger().addHandler(handler)

# # Create an instance of the SDK Client
# domo = Domo(
#     client_id,
#     client_secret,
#     logger_name='foo',
#     log_level=logging.INFO,
#     api_host=api_host)

# dsr = DataSetRequest()
# datasets = domo.datasets

# # Id of the dataset when we upload.
# final_dataset_id = "d69879f6-b655-4756-a7d5-27d10b64a7e7"

# To create a dataset you need to create schema.
# NOTE: Will throw an error if you have wrong # of columns

# data_schema = Schema([
#   Column(ColumnType.LONG, "dealid"),
#   Column(ColumnType.LONG, "status"),
#   Column(ColumnType.STRING, "hearduson"),
#   Column(ColumnType.STRING, "Brand"),
#   Column(ColumnType.STRING, "SubSource"),
#   Column(ColumnType.DATE, "datecr"),
#   Column(ColumnType.DATE, "salesdate"),
#   Column(ColumnType.STRING, "utm_campaign"),
#   Column(ColumnType.STRING, "utm_source"),
#   Column(ColumnType.STRING, "utm_medium"),
#   Column(ColumnType.LONG, "dnc"),
#   Column(ColumnType.STRING, "source"),
#   Column(ColumnType.LONG, "total sold"),
#   Column(ColumnType.LONG, "days"),
# ])

# # Create dataset
# dsr.name = 'MK | Lead Info'
# dsr.description = 'Cleaned data from Raw source.'
# dsr.schema = data_schema

# # Create a DataSet with the given Schema
# dataset = datasets.create(dsr)
# domo.logger.info("Created DataSet " + dataset['id'])
# final_dataset_id = dataset['id']

# retrieved_dataset = datasets.get(dataset['id'])

# # ----- Update a DataSets's metadata ----- #
# update = DataSetRequest()
# update.name = 'MK | Lead Info'
# update.description = 'Cleaned data from Raw source.'
# update.schema = data_schema
# updated_dataset = datasets.update(final_dataset_id, update)

# ---- End Domo Config ------ #

s3 = boto3.client('s3')
BUCKET_NAME = 'fabiano-crm-consys'

bar = Bar('Processing', max=3)
bar.start()

contacts = s3.get_object(Bucket=BUCKET_NAME, Key='DUMP/Deals1.csv')

contacts = pd.read_csv(
    contacts['Body'],
    index_col=0,
    low_memory=False,
    encoding="ISO-8859-1",
)

# Number of dispos
webhooks = s3.get_object(Bucket=BUCKET_NAME, Key='DUMP/WebHook1.csv')

webhooks = pd.read_csv(
    webhooks['Body'],
    index_col=0,
    low_memory=False,
    encoding="ISO-8859-1",
)

us_zip = pd.read_csv(
    './US.csv',
    index_col=False,
    low_memory=False
)

# drop new leads to speed up.
webhooks = webhooks.loc[-webhooks['hook_action'].isin(["NEW LEAD", "NONE/BLANK", "NONE/AHEAD"])]

bar.next()

def clean_campaign(x):
  if pd.isna(x) == False:
    pattern = re.compile("^[\d]*$")
    is_number = bool(pattern.match(x))
    if is_number == False:
      return x.lower()
    else:
      return ''
  else:
    return ''

def clean_status(x):
  if pd.isna(x) == False:
    cleaned = re.findall("^[a-zA-Z09]*[^-]", x)[0]
    return cleaned

def clean_name(x):
  if pd.isna(x) == False:
    return x.lower()

def format_date(date):
  if pd.isna(date) == False:
    string_date = str(date)
    month = string_date[4:6]
    day = string_date[6:8]
    year = string_date[0:4]
    return (f'{month}/{day}/{year}')

def decide_medium(x):
  x = x.lower()
  if 'facebook' in x:
    return "Facebook"
  elif 'call' in x:
    return "Inbound Call"
  else:
    return "Organic / Untracked"

def decide_source(row):
  has_heard = len(row.hearduson) > 0
  has_source = len(row.utm_source) > 0
  has_subsource = len(row.SubSource) > 0
  # Default to what the user says to the sales person on the phone
  if has_heard == True:
    if any(x in row.hearduson for x in ['RADIO','Radio','radio']):
      return 'Radio'
    elif any(x in row.hearduson for x in ["ONLINE SEARCH", "Online Search"]):
      return 'PPC'
    elif any(x in row.hearduson for x in ["FACEBOOK", "Facebook", "facebook"]):
      return 'Facebook'
    elif any(x in row.hearduson for x in ["TV", "Tv", "tv"]):
      return 'TV'
    else:
      return 'Other'
  # If contact has a source attach it
  elif has_source == True:
    if any(x in row.utm_source for x in ["facebook.com","facebook","facebookfronter","facebookfb","facebook-fb","facebook-ig","facebooklaura"]):
      return 'Facebook'
    elif any(x in row.utm_source for x in ["google","bing","youtube","msn","adroll","linkedin"]):
      return 'PPC'
    elif any(x in row.utm_source for x in ["facebookmsg","facebook-messenger"]):
      return 'Social'
    else:
      return 'Other'
  # If contact has a source attach it
  elif has_subsource == True:
    SubSource = row.SubSource.lower()
    if any(x in SubSource for x in ["facebook inbound call"]):
      return 'FB Call'
    elif any(x in SubSource for x in ["facebook"]):
      return 'Facebook'
    elif any(x in SubSource for x in ["google","bing","youtube","msn","adroll","linkedin"]):
      return 'PPC'
    elif any(x in SubSource for x in ["online search"]):
      return 'PPC Call'
    elif any(x in SubSource for x in ["tv"]):
      return 'TV'
    elif any(x in SubSource for x in ["bbb"]):
      return 'BBB Call'
    elif any(x in SubSource for x in ["radio"]):
      return 'Radio'
    else:
      return row['medium']
  else:
    return row['medium']

def reassign(row):
  # User reapplied we need to find the webhook and add the source.
  reapplied = True if row.webhook < 0 else False 
  if reapplied == True:
    return row
  else:
    return row

def add_county(zip):
  if pd.isna(zip) == False:
    pattern = re.compile("^[\d]{5}")
    is_number = bool(pattern.match(zip))
    if is_number == True:
      # find zip in us_zip
      row = us_zip.loc[us_zip['zip'].isin([zip])]
      if len(row['county'].values) > 0:
        # Return county
        return row['county'].values[0]

contacts = contacts[['status', 'hearduson', 'LeadSource', 'SubSource', 'datecr', 'salesdate',  'utm_campaign', 'utm_source', 'utm_medium','dnc', 'sold_tr', 'sold_mt', 'dateASAP', 'HomePhone', 'SecondaryPhone', 'EmailAddress', 'ZipCode', 'State']]
# 'ClientName', 'HomePhone', 'SecondaryPhone', 'EmailAddress', 
# 'NameofResort',
# 'lastdispo',

contacts['medium'] = contacts['LeadSource'].apply(decide_medium)

# Create column for brand instead of source
contacts['LeadSource'].replace([
  'ARMG Website',
  'ARMG Inbound Call',
  'ARMG',
], "ARMG", inplace=True)

contacts['LeadSource'].replace([
  'RNR Website',
  'RNR Inbound Call',
  'RNR WEBSITE',
  'RNR',
], "RNR", inplace=True)

contacts['LeadSource'].replace([
  'RR Website',
  'RR Inbound Call',
  'Facebook-RR',
  'LinkedIn',
], "RR", inplace=True)

contacts['LeadSource'].replace([
  'RET Inbound Call',
  'RET Website',
], "RET", inplace=True)

contacts['LeadSource'].replace([
  'SMART SHEET (2011-2015)',
], "Old Leads", inplace=True)

contacts['LeadSource'].replace([
  'MICRO SITES',
  'Micro Sites',
  'RNR.CA',
  'MICROSITE_TSDR',
], "Micro", inplace=True)

contacts['LeadSource'].replace([
  'Buyology IQ',
  'NKD Media',
  'Thomas',
  'Royal Sands',
  'OTHER - Inbound Call ',
  'OTHER - Inbound Call',
  'Maildrop'
  'Promoter',
], "Other", inplace=True)

contacts['LeadSource'].replace([
  'Referral',
  'ReferAFriend-Page',
], "Referral", inplace=True)

# 'Webhook Marketing'
bar.next()

contacts.rename(columns={'LeadSource': 'Brand'}, inplace=True)

# Clean created date
contacts['dateASAP'] = contacts['dateASAP'].apply(format_date)
contacts['datecr'] = contacts['datecr'].apply(format_date)

# Clean sales date. If sales date exists then we will set sale as true otherwise false
contacts['salesdate'] = contacts['salesdate'].apply(format_date)

# Clean campaign data. Over 800 campaigns, cant parse.
contacts['utm_campaign'] = contacts['utm_campaign'].astype(str)
contacts['utm_campaign'] = contacts['utm_campaign'].apply(clean_campaign)
contacts['utm_campaign'].replace(['nan'], "", inplace=True)

# Heard us on. i.e radio mainly. This is sales person entering in the data.
contacts['hearduson'] = contacts['hearduson'].astype(str)
contacts['hearduson'].replace(['nan'], "", inplace=True)

# Sub source 
contacts['SubSource'] = contacts['SubSource'].astype(str)
contacts['SubSource'].replace(['nan'], "", inplace=True)

# Clean the source for the final source
contacts['utm_source'] = contacts['utm_source'].astype(str)
contacts['utm_source'] = contacts['utm_source'].apply(clean_campaign)
contacts['utm_source'].replace(['nan'], "", inplace=True)

# Just lowercase the medium
contacts['utm_medium'] = contacts['utm_medium'].apply(clean_campaign)

# Make sure the date fields are dates
contacts['dateASAP'] = pd.to_datetime(contacts['dateASAP'])
contacts['datecr'] = pd.to_datetime(contacts['datecr'])
contacts['salesdate'] = pd.to_datetime(contacts['salesdate'])

# If the datecreated and callASAP are diffrent we need to update the source to reflect the new source
contacts['webhook'] = (contacts['datecr'] - contacts['dateASAP']).dt.days

# Replace rows with updated webhook info
contacts = contacts.apply(reassign, axis=1)

# Apply source that is appropriate
contacts['source'] = contacts.apply(decide_source, axis=1)

contacts['total sold'] = contacts['sold_tr'] + contacts['sold_mt']

contacts['county'] = contacts['ZipCode'].apply(add_county)

# Find the difference in days
contacts['days'] = abs((contacts['dateASAP'] - contacts['salesdate']).dt.days)

bar.next()

# Percentage of valid phone #
# Percentage of email address

# Clean dispo name
# contacts['lastdispo'] = contacts['lastdispo'].apply(clean_status)

# listOfSub = pd.DataFrame(contacts['SubSource'].unique())
# listOfSub.to_csv("export-sub.csv")

# # Number of dispos
# dispos = s3.get_object(Bucket=BUCKET_NAME, Key='DUMP/Dispo1.csv')

# dispos = pd.read_csv(
#     dispos['Body'],
#     index_col=0,
#     low_memory=False,
#     encoding="ISO-8859-1",
# )
# contacts['Dispo Count'] = pd.Series(contacts.index, index=contacts.index).map(dispos['dealid'].value_counts())

contacts = contacts.drop(columns=['sold_tr', 'sold_mt', 'medium'])

# Save the final product as a csv. Useful for testing the data is coming out.
contacts.to_csv('export-mk.csv')
# print(contacts.dtypes)

# final = contacts.to_csv(header=False)

# datasets.data_import(final_dataset_id, final)
bar.finish()