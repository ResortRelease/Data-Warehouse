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

# Prompt user for type
response = input("Please enter which type of list (SMS, EMAIL) \n >>> ")

def load_contacts():
  ##### Import Deals Sheet #####
  # Get file from S3.
  contacts = s3.get_object(Bucket=BUCKET_NAME, Key='DUMP/Deals1.csv')

  # Load it into dataframe without saving
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
  return contacts

# Let only import numbers from phone columns
def clean_number(number):
  pattern = re.compile("^(\+\d{1,2}\s)?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}$")
  if pd.isna(number) == False:
    phone_exists = bool(pattern.match(number))
    if phone_exists == True:
      return number
    else:
      return "Not a valid phone"
  else:
    return "Not a valid phone"

# Function for readiblity
def clean_email(email):
  pattern = re.compile("(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)")
  exclude_domains = re.compile("(^[a-zA-Z0-9_.+-]+@[yahoo|aol]+\.[a-zA-Z0-9-.]+$)")
  if pd.isna(email) == False:
    email_exists = bool(pattern.match(email))
    is_yahoo = bool(pattern.match(email))
    if email_exists == True:
      email = email.lower()
      email = email.strip()
      return email
    else:
      return "Not a valid email"
  else:
    return "Not a valid email"

# Format the dates to MM/DD/YYYY
def format_date(date):
  if pd.isna(date) == False:
    string_date = str(date)
    month = string_date[4:6]
    day = string_date[6:8]
    year = string_date[0:4]
    return (f'{month}/{day}/{year}')
  else:
    return False

def clean_status(x):
  if pd.isna(x) == False:
    cleaned = re.findall("^[a-zA-Z09]*[^-]", x)[0]
    return cleaned

def clean_client_name(x):
  if pd.isna(x) == False:
    x = x.replace(r"\(.*\)","") # Remove "(anything)"
    string_array = x.split(" ") # Create array of strings
    name_length = len(string_array) # Count the length of array
    fullname = ''
    if(name_length == 1):
      fullname = string_array[0]
    elif (name_length == 2):
      fullname = string_array[0] + " " + string_array[1]
    elif (name_length == 3):
      fullname = string_array[0] + " " + string_array[2]
    elif (name_length > 4):
      fullname = string_array[0] + " " + string_array[name_length - 1]

    cleaned = fullname.title()
    return cleaned

def clean_name(x):
  if pd.isna(x) == False:
    # We are going to get rid of everything after the 'and' + '&' so it looks like a name.
    return x.title()

def segment_list():
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

def generate_email_list():
  contacts = load_contacts()
  print("\n Initial:", contacts.shape[0])

  bar.next()
  # Clean phone number
  contacts['HomePhone'] = contacts['HomePhone'].apply(clean_number)

  # Clean emails
  contacts['EmailAddress'] = contacts['EmailAddress'].apply(clean_email)

  # Drop "Not a valid email"
  contacts = contacts.loc[-contacts['EmailAddress'].isin(["Not a valid email"])]
  print("\n Removed emails:", contacts.shape[0])

  # Clean created date
  contacts['datecr'] = contacts['datecr'].apply(format_date)

  # Clean sales date
  contacts['salesdate'] = contacts['salesdate'].apply(format_date)

  # Drop if there is a sale date
  contacts = contacts.loc[contacts['salesdate'].isin([False])]
  print("\n Removed sales:", contacts.shape[0])

  bar.next()

  # Clean dispo name
  contacts['lastdispo'] = contacts['lastdispo'].apply(clean_status)

  # Clean the names of clients and resorts
  contacts['Full Name'] = contacts['ClientName'].apply(clean_client_name)
  contacts['First Name'], contacts['Last Name'] = contacts['Full Name'].str.split(' ', 1).str

  contacts['NameofResort'] = contacts['NameofResort'].apply(clean_name)

  # Make columns string
  contacts['hasform'] = contacts['hasform'].astype(str)
  contacts['dnc'] = contacts['dnc'].astype(str)

  # Drop if the last dispo is terminate
  contacts = contacts[contacts["lastdispo"].str.contains('DNC|WRONG|GOTRID|AWC|CANT|NIT')==False]
  contacts = contacts.loc[-contacts['lastdispo'].isin([False])]
  print("\n Removed terminated:", contacts.shape[0])

  # Merge duplicates
  contacts = contacts.groupby('EmailAddress').agg({
                              'datecr': 'first', 
                              'salesdate': 'any', 
                              'Full Name': 'first', 
                              'First Name': 'first', 
                              'Last Name': 'first', 
                              'NameofResort': 'first', 
                              'lastdispo': 'first', 
                              'hasform': ', '.join, 
                              'status': ', '.join, 
                              'dnc':', '.join})

  # Drop dnc with 1 
  contacts = contacts[contacts["dnc"].str.contains('1')==False]
  contacts = contacts.loc[-contacts['dnc'].isin([False])]
  print("\n Removed DNC:", contacts.shape[0])

  # # Optional: Filter down to users who have been contacted recently
  # contacts = contacts[contacts["hasform"].str.contains('nan')==False]
  # contacts = contacts.loc[contacts['hasform'].isin([False])]

  contacts = contacts[contacts["status"].str.contains('1|2')==True]
  contacts = contacts.loc[-contacts['status'].isin([False])]
  print("\n Only status 1 and 2:", contacts.shape[0])

  contacts.to_csv('export-email.csv')

  bar.next()
  bar.finish()

def generate_phone_list(contacts):
  bar.next()
  contacts = contacts['HomePhone'].apply(clean_number)
  bar.next()
  contacts = contacts.loc[-contacts['HomePhone'].isin(["Not a valid phone"])]
  contacts.to_csv('export-phone.csv')
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