import pandas as pd
import numpy as np
from datetime import datetime
from progress.bar import Bar

# Secure access to domo
import secret

# General functions for RR
import rr_fun

import boto3 #AWS
import botocore

s3 = boto3.client('s3')
BUCKET_NAME = 'fabiano-crm-consys'

bar = Bar('Processing', max=3)
bar.start()

# Import deals as contacts
contacts = s3.get_object(Bucket=BUCKET_NAME, Key='DUMP/Deals1.csv')

contacts = pd.read_csv(
    contacts['Body'],
    # './Imports/Deals1.csv',
    index_col=False,
    low_memory=False,
    encoding="ISO-8859-1",
)

bar.next()

# Remove all columns except the following
# contacts = contacts[['dealid', 'hearduson', 'LeadSource', 'SubSource', 'datecr', 'salesdate', 'dnc', 'sold_tr', 'sold_mt', 'dateASAP', 'HomePhone', 'SecondaryPhone', 'EmailAddress', 'ZipCode', 'State', 'hasform']]

# Scrape details from leadsource to apply to medium
contacts['medium'] = contacts['LeadSource'].apply(rr_fun.decide_medium)

# Create column for brand instead of source
contacts['LeadSource'].replace([
  'ARMG Website',
  'ARMG Inbound Call',
  'ARMG INBOUND CALL',
  'ARMG',
], "ARMG", inplace=True)

contacts['LeadSource'].replace([
  'RNR Website',
  'RNR Inbound Call',
  'RNR WEBSITE',
  'REDEMPTION',
  'REDEMPTION INBOUND CALL',
  'RNR',
], "RNR", inplace=True)

contacts['LeadSource'].replace([
  'RR Website',
  'RR WEBSITE',
  'RR Inbound Call',
  'RESORT RELEASE INBOUND CALL',
  'RESORT RELEASE',
  'Resort Release',
  'FACEBOOK INBOUND CALL',
  'Facebook-RR',
  'LinkedIn',
], "RR", inplace=True)

contacts['LeadSource'].replace([
  'RET Inbound Call',
  'RET Website',
  'Facebook-RET',
  'RESORT EXIT TEAM',
  'EXIT TEAM INBOUND CALL',
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
  'Maildrop',
  'Promoter',
], "Other", inplace=True)

contacts['LeadSource'].replace([
  'Referral',
  'ReferAFriend-Page',
], "Referral", inplace=True)

# 'Webhook Marketing'
bar.next()

# LeadSource was cleaned and parsed and now is 
contacts.rename(columns={'LeadSource': 'Brand'}, inplace=True)

# Clean created/asap date
contacts['dateASAP'] = contacts['dateASAP'].apply(rr_fun.format_date)
contacts['datecr'] = contacts['datecr'].apply(rr_fun.format_date)

# Clean sales date. If sales date exists then we will set sale as true otherwise false
contacts['salesdate'] = contacts['salesdate'].apply(rr_fun.format_date)

# Clean campaign data. Over 800 campaigns, cant parse.
contacts['utm_campaign'] = contacts['utm_campaign'].astype(str)
contacts['utm_campaign'] = contacts['utm_campaign'].apply(rr_fun.clean_campaign)
contacts['utm_campaign'].replace(['nan'], "", inplace=True)

# Heard us on. i.e radio mainly. This is sales person entering in the data.
contacts['hearduson'] = contacts['hearduson'].astype(str)
contacts['hearduson'].replace(['nan'], "", inplace=True)

# Sub source 
contacts['SubSource'] = contacts['SubSource'].astype(str)
contacts['SubSource'] = contacts['SubSource'].str.lower()
contacts['SubSource'].replace(['nan'], "", inplace=True)

# Clean the source for the final source
contacts['utm_source'] = contacts['utm_source'].astype(str)
contacts['utm_source'] = contacts['utm_source'].apply(rr_fun.clean_campaign)
contacts['utm_source'].replace(['nan'], "", inplace=True)

# Just lowercase the medium
contacts['utm_medium'] = contacts['utm_medium'].apply(rr_fun.clean_campaign)

# Make sure the date fields are dates
contacts['dateASAP'] = pd.to_datetime(contacts['dateASAP'])
contacts['datecr'] = pd.to_datetime(contacts['datecr'])
contacts['salesdate'] = pd.to_datetime(contacts['salesdate'])

# If the datecreated and callASAP are diffrent we need to update the source to reflect the new source
contacts['webhook'] = (contacts['datecr'] - contacts['dateASAP']).dt.days

# Replace rows with updated webhook info
contacts = contacts.apply(rr_fun.reassign, axis=1)

# Apply source that is appropriate
contacts['source'] = contacts.apply(rr_fun.decide_source, axis=1)

# Count total sold
contacts['total sold'] = contacts['sold_tr'] + contacts['sold_mt']

# Deal id is converted into base64 to make it easier to find on the crm.
contacts['base64'] = contacts.index.map(str)
contacts['base64'] = contacts['base64'].apply(rr_fun.stringToBase64)

# Find the difference in days
contacts['days'] = abs((contacts['dateASAP'] - contacts['salesdate']).dt.days)

# Number of dispos
dispos = s3.get_object(Bucket=BUCKET_NAME, Key='DUMP/Dispo1.csv')
dispos = pd.read_csv(
    dispos['Body'],
    index_col=0,
    low_memory=False,
    encoding="ISO-8859-1",
)

# How many dispos each client has
contacts['Dispo Count'] = pd.Series(contacts.index, index=contacts.index).map(dispos['dealid'].value_counts())

# If sold 1 else 0
contacts['was sold'] = contacts["salesdate"].apply(rr_fun.was_sold)

# Remove some extra fields
contacts = contacts[["dealid", "status", "Brand", "source", "SubSource", "datecr", "dateASAP", "salesdate", "cancelsale", "utm_term", "utm_campaign", "utm_source", "utm_medium", "utm_content", "hearduson", "total sold", "base64", "days", "was sold"]]

# Save the final product as a csv.
# contacts.to_csv('./Exports/export-mk.csv')

final_csv = contacts.to_csv(None)
s3_resource = boto3.resource('s3')
s3_resource.Object('rr-data-test', 'export-mk.csv').put(Body=final_csv)
bar.next()

bar.finish()