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
    './imports/experian.csv',
    index_col=False,
    low_memory=False,
    encoding="ISO-8859-1",
)

# Import latest deals file
contacts = s3.get_object(Bucket=BUCKET_NAME, Key='DUMP/Deals1.csv')
contacts = pd.read_csv(
    contacts['Body'],
    index_col=False,
    low_memory=False,
    encoding="ISO-8859-1",
)

# Drop extra fields
contacts = contacts[['dealid', 'ClientName', 'StreetAddress', 'ZipCode', 'City', 'State', 'HomePhone', 'SecondaryPhone', 'EmailAddress', 'status']]

# Only working with lead cloud status
contacts = contacts[contacts["status"].str.contains('1|2|5a')==True]
contacts = contacts.loc[-contacts['status'].isin([False])]
all_lead = contacts['dealid'].unique().tolist()


# ========= Experian ==========

# Only dealids with that match status
experian = experian.loc[experian['DealId'].isin(all_lead)]

# Remove rows without address
experian = experian.dropna(subset=['RA RETURNED A1'])
experian = experian.dropna(subset=['EMAILADDRESS'])

# Lowercase email and address
experian['EMAILADDRESS'] = experian['EMAILADDRESS'].str.lower()
experian['RA RETURNED A1'] = experian['RA RETURNED A1'].str.lower()

# Get unique phone and emails and save in memory
unique_email = experian['EMAILADDRESS'].unique().tolist()
unique_phone = experian['PHONE'].unique().tolist()
unique_address = experian['RA RETURNED A1'].unique().tolist()

# Clean name and addresses
experian['Clean Name'] = experian['Enhanced'].apply(rr_fun.clean_client_name)
experian['First Name'], experian['Last Name'] = experian['Clean Name'].str.split(' ', 1).str
experian['RA RETURNED A1'] = experian['RA RETURNED A1'].str.title()
experian['RA RETURNED A2'] = experian['RA RETURNED A2'].str.title()
experian['RA RETURNED CITY'] = experian['RA RETURNED CITY'].str.title()

experian = experian[['DealId','RA RETURNED A1', 'RA RETURNED CITY', 'RA RETURNED STATE', 'RA RETURNED ZIP', ]]
experian.to_csv('./Exports/export-experian.csv')

list_of_experian = experian['DealId'].unique().tolist()
print("Experian", len(list_of_experian))


# ============ CRM ============ 

# Clean names
contacts['Full Name'] = contacts['ClientName'].apply(rr_fun.clean_client_name)
contacts['First Name'], contacts['Last Name'] = contacts['Full Name'].str.split(' ', 1).str

# Lowercase email and address
contacts['EmailAddress'] = contacts['EmailAddress'].str.lower()
contacts['StreetAddress'] = contacts['StreetAddress'].str.lower()

print("Entire:", contacts.shape[0])
# Drop leads that already exist in experian
contacts = contacts.loc[-contacts['dealid'].isin(list_of_experian)]
print("Entire minus experian:", contacts.shape[0])

# Drop rows with blank address
contacts = contacts.dropna(subset=['StreetAddress'])
print("No Street Address:", contacts.shape[0])

# A lot of dirt
contacts = contacts[contacts["StreetAddress"].str.contains('will provide|Refused')==False]
contacts = contacts.loc[-contacts['StreetAddress'].isin([False])]
print("Without Dirt:", contacts.shape[0])

# Eh? No canada
contacts = contacts[contacts["City"].str.contains('Canada|CANADA|canada')==False]
contacts = contacts[contacts["State"].str.contains('Canada|CANADA|canada')==False]
contacts = contacts.loc[-contacts['City'].isin([False])]
contacts = contacts.loc[-contacts['State'].isin([False])]
print("No Canada:", contacts.shape[0])

# Clean phone number to only digits
contacts['HomePhone'] = contacts['HomePhone'].apply(rr_fun.clean_number)
contacts['SecondaryPhone'] = contacts['SecondaryPhone'].apply(rr_fun.clean_number)

# Remove rows that match the list of phone # and emails
contacts = contacts.loc[-contacts['HomePhone'].isin(unique_phone)]
print("Home:", contacts.shape[0])

contacts = contacts.loc[-contacts['SecondaryPhone'].isin(unique_phone)]
print("Secondary:", contacts.shape[0])

contacts = contacts.loc[-contacts['EmailAddress'].isin(unique_email)]
print("Email:", contacts.shape[0])

# Lowercase email and address
contacts['StreetAddress'] = contacts['StreetAddress'].str.lower()

# Drop address that show up in Experian
contacts = contacts.loc[-contacts['StreetAddress'].isin(unique_address)]

# Title the streets
contacts['StreetAddress'] = contacts['StreetAddress'].str.title()
print("Address:", contacts.shape[0])

print("Only 1,2,5a:", contacts.shape[0])
print(len(contacts['StreetAddress'].unique().tolist()))
print(len(contacts['HomePhone'].unique().tolist()))
print(len(contacts['EmailAddress'].unique().tolist()))

contacts = contacts[['dealid','StreetAddress', 'City', 'State', 'ZipCode']]
contacts.to_csv('./Exports/export-addresses.csv')