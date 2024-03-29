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

# What are we looking for? [email, phone]
# Which status should be included? Provide a list by commas.

# Prompt user for type
response = input("Please enter which type of list (SMS, EMAIL) \n >>> ")

def load_contacts():
  ##### Import Deals Sheet #####
  # Get file from S3.
  contacts = s3.get_object(Bucket=BUCKET_NAME, Key='DUMP/Deals1.csv')

  # # Load it into dataframe without saving
  contacts = pd.read_csv(
      contacts['Body'],
      index_col=False,
      low_memory=False,
      encoding="ISO-8859-1",
  )

  # We only need a handful of columns
  # contacts.drop(columns=['status', 'datecr', 'salesdate', 'ClientName', 'NameofResort', 'HomePhone', 'EmailAddress', 'lastdispo', 'hasform', 'dnc'])
  contacts = contacts.drop(columns=['LeadSource', 'SubSource', 'timestamp', 'timeASAP', 'aweberid', 'salesfusionid', 'mortgage', 'appsetdate', 'appverdate', 'cancelsale', 'holdsale', 'sold_tr', 'sold_mt', 'sold_tr_rev', 'sold_mt_rev', 'CLOSER_REP', 'VERIFY', 'StreetAddress', 'City', 'State', 'ZipCode', 'SecondaryPhone', 'timezone', 'reserve', 'reservetime',  'lastfronter', 'lastdispodate', 'emergency', 'utm_term', 'utm_campaign', 'utm_source', 'utm_medium', 'utm_content', 'hasapp', 'sold_tr_rev_net', 'sold_mt_rev_net', 'hearduson', 'heardother', 'sold_tr1', 'sold_mt1', 'tfdb_case', 'qareport', 'qareport1', 'MQL', 'SQL', 'SQT', 'Nurture'])

  return contacts

def fronter_name(name=None, full=False): 
  pattern = re.compile("(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)")
  if pd.isna(name) == False:
    return name
  else:
    return "Taylor" 

def fronter_full_name(name): 
  pattern = re.compile("(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)")
  if pd.isna(name) == False:
    if name != "SYSTEM":
      # Seperate last letter
      name = name[:-1].capitalize()
      last_name = name[-1:].capitalize()
      return name + " " + last_name + "."
    else:
      return "Taylor M." 
  else:
    return "Taylor M." 

def segment_list():
  # Drop dnc with 1 
  contacts = contacts[contacts["dnc"].str.contains('1')==False]
  contacts = contacts.loc[-contacts['dnc'].isin([False])]

  # Drop if there is a sale date
  contacts = contacts.loc[contacts['salesdate'].isin([False])]

  # Drop if the last dispo is terminate
  contacts = contacts[contacts["lastdispo"].str.contains('DNC|WRONG|GOTRID|AWC|CANT|NIT')==False]
  contacts = contacts.loc[-contacts['lastdispo'].isin([False])]

  # # Optional: Filter down to users who have been contacted recently
  # contacts = contacts[contacts["hasform"].str.contains('nan')==False]
  # contacts = contacts.loc[-contacts['hasform'].isin([False])]

  contacts = contacts[contacts["status"].str.contains('1|2')==True]
  contacts = contacts.loc[-contacts['status'].isin([False])]

  contacts = contacts[contacts["Group"].str.contains('47|55')==True]
  contacts = contacts.loc[contacts['Group'].isin([True])]

def generate_email_list():
  contacts = load_contacts()
  print("\n Initial:", contacts.shape[0])

  bar.next()
  # Clean phone number
  contacts['HomePhone'] = contacts['HomePhone'].apply(rr_fun.clean_number)

  # Clean emails
  contacts['EmailAddress'] = contacts['EmailAddress'].apply(rr_fun.clean_email)

  # Drop "Not a valid email"
  contacts = contacts.loc[-contacts['EmailAddress'].isin(["Not a valid email"])]
  print("\n Removed emails:", contacts.shape[0])

  # Clean dates
  contacts['datecr'] = contacts['datecr'].apply(rr_fun.format_date)
  contacts['dateASAP'] = contacts['dateASAP'].apply(rr_fun.format_date)
  contacts['salesdate'] = contacts['salesdate'].apply(rr_fun.format_date)

  # Drop if there is a sale date
  contacts = contacts.loc[contacts['salesdate'].isin([False])]
  print("\n Removed sales:", contacts.shape[0])

  bar.next()

  # Clean dispo name
  contacts['lastdispo'] = contacts['lastdispo'].apply(rr_fun.clean_status)

  # Clean the names of clients and resorts
  contacts['Full Name'] = contacts['ClientName'].apply(rr_fun.clean_client_name)
  contacts['First Name'], contacts['Last Name'] = contacts['Full Name'].str.split(' ', 1).str

  contacts['NameofResort'] = contacts['NameofResort'].apply(rr_fun.clean_name)

  # Make columns string
  contacts['hasform'] = contacts['hasform'].astype(str)
  contacts['dnc'] = contacts['dnc'].astype(str)

  # Drop if the last dispo is terminate
  contacts = contacts[contacts["lastdispo"].str.contains('DNC|WRONG|GOTRID|AWC|CANT|NIT')==False]
  contacts = contacts.loc[-contacts['lastdispo'].isin([False])]
  print("\n Removed terminated:", contacts.shape[0])

  # # last fronter
  # contacts['Agent Full'] = contacts['reserveuser'].apply(fronter_full_name)
  # contacts['Agent First'] = contacts['reserveuser'].apply(fronter_name)

  contacts['Group'] = contacts['Group'].astype(str)
  contacts = contacts[contacts["Group"].str.contains('47|55')==True]
  contacts = contacts.loc[-contacts['Group'].isin([False])]

  # Merge duplicates
  contacts = contacts.groupby('EmailAddress').agg({
                              'dealid': 'first',
                              'datecr': 'first', 
                              'dateASAP': 'first', 
                              'salesdate': 'any', 
                              'Full Name': 'first', 
                              'First Name': 'first', 
                              'Last Name': 'first', 
                              'HomePhone': 'first', 
                              'hasform': ', '.join, 
                              'status': ', '.join, 
                              'Group': ', '.join, 
                              'dnc':', '.join})

  contacts['dnc'] = contacts['dnc'].astype(str)
  # Drop dnc with 1 
  contacts = contacts[contacts['dnc'].str.contains('1')==False]
  contacts = contacts.loc[-contacts['dnc'].isin([False])]
  print("\n Removed DNC:", contacts.shape[0])

  # # Optional: Filter down to users who have been contacted recently
  # contacts = contacts[contacts["hasform"].str.contains('nan')==False]
  # contacts = contacts.loc[contacts['hasform'].isin([False])]

  contacts['status'] = contacts['status'].astype(str)
  contacts = contacts[contacts["status"].str.contains('1|2')==True]
  contacts = contacts.loc[-contacts['status'].isin([False])]
  print("\n Only status 1 and 2:", contacts.shape[0])

  # aggregate function messes up the index
  contacts = contacts.reset_index()

  dispo = s3.get_object(Bucket="rr-data-test", Key='last-dispo.csv')
  
  lastdispo = pd.read_csv(
      dispo['Body'],
      # './Exports/finaldispos.csv',
      index_col=False,
      low_memory=False,
      encoding="ISO-8859-1",
  )

  contacts = pd.merge(contacts, lastdispo, how='inner',
        left_on='dealid', right_on='dealid')

  contacts['Fronter First'] = contacts['Fronter First'].apply(fronter_name)
  contacts['Fronter Last'] = contacts['Fronter Last'].apply(fronter_name)

  contacts['agent-full'] = contacts['Fronter First'].astype(str) + " " + contacts['Fronter Last']

  contacts = contacts[['dealid', 'EmailAddress', 'HomePhone', 'datecr', 'dateASAP', 'agent-full', 'Fronter First', 'Fronter Last', 'First Name', 'status', 'Group']]

  contacts.to_csv('./Exports/export-email.csv', index=False)

  bar.next()
  bar.finish()

def generate_phone_list():
  contacts = load_contacts()
  print("\n Initial:", contacts.shape[0])

  bar.next()
  # Clean phone number
  contacts['HomePhone'] = contacts['HomePhone'].apply(rr_fun.clean_number)
  contacts = contacts.loc[-contacts['HomePhone'].isin(["Not a valid phone"])]

  # Clean created date
  contacts['datecr'] = contacts['datecr'].apply(rr_fun.format_date)
  contacts['dateASAP'] = contacts['dateASAP'].apply(rr_fun.format_date)

  # last fronter
  contacts['Last Contacted'] = contacts['reserveuser'].apply(fronter_name)

  bar.next()

  # Clean dispo name
  contacts['lastdispo'] = contacts['lastdispo'].apply(rr_fun.clean_status)

  # Clean the names of clients and resorts
  contacts['Full Name'] = contacts['ClientName'].apply(rr_fun.clean_client_name)
  contacts['First Name'], contacts['Last Name'] = contacts['Full Name'].str.split(' ', 1).str

  contacts['NameofResort'] = contacts['NameofResort'].apply(rr_fun.clean_name)

  # Make columns string
  contacts['dnc'] = contacts['dnc'].astype(str)

  # Drop if the last dispo is terminate
  contacts = contacts[contacts["lastdispo"].str.contains('DNC|WRONG|GOTRID|AWC|CANT|NIT')==False]
  contacts = contacts.loc[-contacts['lastdispo'].isin([False])]
  print("\n Removed terminated:", contacts.shape[0])

  # Merge duplicates
  contacts = contacts.groupby('HomePhone').agg({
                              'dealid': 'first',
                              'datecr': 'first', 
                              'dateASAP': 'first', 
                              'First Name': 'first', 
                              'lastdispo': 'first', 
                              'status': ', '.join, 
                              'dnc':', '.join})

  # Drop dnc with 1 
  contacts = contacts[contacts["dnc"].str.contains('1')==False]
  contacts = contacts.loc[-contacts['dnc'].isin([False])]
  print("\n Removed DNC:", contacts.shape[0])

  contacts = contacts[contacts["status"].str.contains('1|2')==True]
  contacts = contacts.loc[-contacts['status'].isin([False])]
  print("\n Only status 1 and 2:", contacts.shape[0])

  contacts['Group'] = contacts['Group'].astype(str)
  contacts = contacts[contacts["Group"].str.contains('47|55')==True]
  contacts = contacts.loc[-contacts['Group'].isin([False])]

  # aggregate function messes up the index
  contacts = contacts.reset_index()

  dispo = s3.get_object(Bucket="rr-data-test", Key='last-dispo.csv')

  lastdispo = pd.read_csv(
      dispo['Body'],
      # './Exports/finaldispos.csv',
      index_col=False,
      low_memory=False,
      encoding="ISO-8859-1",
  )

  contacts = pd.merge(contacts, lastdispo, how='inner',
        left_on='dealid', right_on='dealid')
  
  contacts[['First Name']] = contacts[['First Name']].fillna(value="there")

  contacts = contacts[['dealid', 'HomePhone', 'datecr', 'dateASAP', 'Fronter First', 'Fronter Last', 'First Name', 'status']]

  contacts.to_csv('./Exports/export-phone.csv', index=False)
  bar.next()
  bar.finish()

# Generate progress bar and start it
bar = Bar('Processing', max=3)
bar.start()

# Load from AWS

if response.lower() == 'email':
  generate_email_list()
elif response.lower() == 'sms':
  generate_phone_list()
else:
  print('Try again...')