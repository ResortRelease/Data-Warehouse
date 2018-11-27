import re
import logging
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

# Import experian data
experian = pd.read_csv(
    './experian.csv',
    index_col=False,
    low_memory=False,
    encoding="ISO-8859-1",
)

# Remove rows without address
experian = experian.dropna(subset=['RA RETURNED A1'])

# Lowercase email and address
experian['EMAILADDRESS'] = experian['EMAILADDRESS'].str.lower()
experian['RA RETURNED A1'] = experian['RA RETURNED A1'].str.lower()

# Get unique phone and emails and save in memory
unique_email = experian['EMAILADDRESS'].unique().tolist()
unique_phone = experian['PHONE'].unique().tolist()
unique_address = experian['RA RETURNED A1'].unique().tolist()

# Import latest deals file
contacts = s3.get_object(Bucket=BUCKET_NAME, Key='DUMP/Deals1.csv')

# Load it into dataframe without saving
contacts = pd.read_csv(
    contacts['Body'],
    index_col=0,
    low_memory=False,
    encoding="ISO-8859-1",
)

# County info
# contacts['county'] = contacts['ZipCode'].apply(rr_fun.add_county)

contacts = contacts[['ClientName', 'StreetAddress', 'ZipCode', 'City', 'State', 'HomePhone', 'SecondaryPhone', 'EmailAddress', 'status']]

contacts['Full Name'] = contacts['ClientName'].apply(rr_fun.clean_client_name)
contacts['First Name'], contacts['Last Name'] = contacts['Full Name'].str.split(' ', 1).str

# Lowercase email and address
contacts['EmailAddress'] = contacts['EmailAddress'].str.lower()
contacts['StreetAddress'] = contacts['StreetAddress'].str.lower()

# Drop rows with blank address
contacts = contacts.dropna(subset=['StreetAddress'])

# A lot of dirt
contacts = contacts[contacts["StreetAddress"].str.contains('will provide|Refused')==False]
contacts = contacts.loc[-contacts['StreetAddress'].isin([False])]

# Clean phone number to only digits
contacts['HomePhone'] = contacts['HomePhone'].apply(rr_fun.clean_number)
contacts['SecondaryPhone'] = contacts['SecondaryPhone'].apply(rr_fun.clean_number)

print("Entire", contacts.shape[0])

# Remove rows that match the list of phone # and emails
contacts = contacts.loc[-contacts['HomePhone'].isin(unique_phone)]
print("Home", contacts.shape[0])

contacts = contacts.loc[-contacts['SecondaryPhone'].isin(unique_phone)]
print("Secondary", contacts.shape[0])

contacts = contacts.loc[-contacts['EmailAddress'].isin(unique_email)]
print("Email", contacts.shape[0])

contacts = contacts.loc[-contacts['StreetAddress'].isin(unique_address)]
print("Address", contacts.shape[0])

contacts = contacts[contacts["status"].str.contains('0b|1|2|5a')==True]
contacts = contacts.loc[-contacts['status'].isin([False])]

print("Only 0b,1,2,5a", contacts.shape[0])
print(len(contacts['StreetAddress'].unique().tolist()))
print(len(contacts['HomePhone'].unique().tolist()))
print(len(contacts['EmailAddress'].unique().tolist()))

# Sanity test
contacts = contacts[['EmailAddress', 'HomePhone', 'ClientName', 'First Name', 'Last Name', 'StreetAddress', 'City', 'State', 'ZipCode']]
contacts.to_csv('export-addresses.csv')

# Clean name and addresses
experian['RA RETURNED FIRST NAME'] = experian['RA RETURNED FIRST NAME'].str.title()
experian['RA RETURNED LAST NAME'] = experian['RA RETURNED LAST NAME'].str.title()
experian['RA RETURNED A1'] = experian['RA RETURNED A1'].str.title()
experian['RA RETURNED A2'] = experian['RA RETURNED A2'].str.title()
experian['RA RETURNED CITY'] = experian['RA RETURNED CITY'].str.title()

experian = experian[['EMAILADDRESS', 'PHONE', 'NAME', 'RA RETURNED FIRST NAME', 'RA RETURNED LAST NAME', 'RA RETURNED A1', 'RA RETURNED CITY', 'RA RETURNED STATE', 'RA RETURNED ZIP']]
experian.to_csv('export-experian.csv')