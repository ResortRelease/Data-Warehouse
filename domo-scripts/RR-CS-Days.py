import secret
import logging
from io import StringIO
from datetime import datetime
import pandas as pd
import numpy as np
from progress.bar import Bar

import boto3 #AWS
import botocore

from pydomo import Domo

from pydomo.datasets import DataSetRequest, Schema, Column, ColumnType, Policy
from pydomo.datasets import PolicyFilter, FilterOperator, PolicyType, Sorting, UpdateMethod

s3 = boto3.client('s3')

BUCKET_NAME = 'fabiano-crm-consys'  # replace with your bucket name

#### Domo Config #####

# Build an SDK configuration
client_id = secret.domo_id
client_secret = secret.domo_secret
api_host = 'api.domo.com'

# Configure the logger
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logging.getLogger().addHandler(handler)

# Create an instance of the SDK Client
domo = Domo(
    client_id,
    client_secret,
    logger_name='foo',
    log_level=logging.INFO,
    api_host=api_host)

dsr = DataSetRequest()
datasets = domo.datasets

# Id of the dataset when we upload.
final_dataset_id = "27fecc96-5313-485a-9cb2-31874c7c41a8"

# To create a dataset you need to create schema.
# NOTE: Will throw an error if you have wrong # of columns

data_schema = Schema([
  Column(ColumnType.STRING, "propertyid"),
  Column(ColumnType.LONG, "Check Requested"),
  Column(ColumnType.LONG, "Closed"),
  Column(ColumnType.LONG, "Doc Date"),
  Column(ColumnType.LONG, "Estoppel"),
  Column(ColumnType.LONG, "Execution"),
  Column(ColumnType.LONG, "Inventory"),
  Column(ColumnType.LONG, "Inventory - Active"),
  Column(ColumnType.LONG, "Inventory - New"),
  Column(ColumnType.LONG, "Inventory - Ready to Relist"),
  Column(ColumnType.LONG, "Inventory - Scheduled"),
  Column(ColumnType.LONG, "Inventory - Unsold"),
  Column(ColumnType.LONG, "Purchase Agreement"),
  Column(ColumnType.LONG, "Sale Date"),
  Column(ColumnType.LONG, "Transfered"),
  Column(ColumnType.LONG, "Welcome"),
  Column(ColumnType.STRING, "Status"),
  Column(ColumnType.STRING, "Sold"),
  Column(ColumnType.STRING, "Property Type"),
  Column(ColumnType.LONG, "Since Update"),
  Column(ColumnType.STRING, "Resort Family"),
  Column(ColumnType.LONG, "totalDays"),
  Column(ColumnType.DATE, "Start Date"),
])

# # Create dataset
# dsr.name = 'CS | Status by Days'
# dsr.description = 'Dispos for RR All Properties'
# dsr.schema = data_schema

# # Create a DataSet with the given Schema
# dataset = datasets.create(dsr)
# domo.logger.info("Created DataSet " + dataset['id'])
# final_dataset_id = dataset['id']

# retrieved_dataset = datasets.get(dataset['id'])

# # ----- Update a DataSets's metadata ----- #
# update = DataSetRequest()
# dsr.name = 'CS | Status by Days'
# dsr.description = 'Dispos for RR All Properties'
# dsr.schema = data_schema
# updated_dataset = datasets.update(final_dataset_id, update)

# ---- End Domo Config ------ #

bar = Bar('Processing', max=3)
bar.start()

##### Import Property #####
# Get file from S3. Note: Double period is not a typo...
propertyFile = s3.get_object(Bucket=BUCKET_NAME, Key='DUMP/Properties1..csv')
bar.next()  # Annoying to have to wait. Little status update.

# Load it into dataframe without saving
properties = pd.read_csv(propertyFile['Body'], index_col=0, low_memory=False)

# statusarray cant be blank. Set the column to string type
properties['statusarray'] = properties['statusarray'].astype(str)

# Remove Junk properties. (Cancelled, Not Paid, etc)
properties = properties.loc[-properties['status'].isin(["0", "1d", "1e", "1f", "1g", "1h", "1i", "1l", "1m", "1n"])]

# We only can work with transfers so remove morgage
# properties = properties.loc[properties['mt_tr'].isin(["TRANSFER"])]
# properties = properties.loc[properties['inv_deed'].isin(["Inventory"])]
properties['resortfamily'] = properties['resortfamily'].str.title()

# Make a list of the properties that match the requirements
listOfProperties = properties.index.tolist()

# Unwind "statusarray" into status as new dispos
# propertyid, dispo, cleanDate
newDispos = []
for index, row in properties.iterrows():
    if (len(row['statusarray']) > 1):
        newDispos.append([index, "Sale Date", row['startdate']])
        newDispos.append([index, "Doc Date", row['doc_date']])
        newDispos.append([index, "Today", "20180822"])
        arrayOfStatus = row['statusarray'].split("|")
        if (len(arrayOfStatus) > 1):
            for singleStatus in arrayOfStatus[1:]:
                statusToAdd = singleStatus.split(",")
                statusToAdd.insert(0, index)
                newDispos.append(statusToAdd)

# New dataframe with the property dispos
generatedDispos = pd.DataFrame(newDispos, columns=["propertyid", "dispo", 'date', 'idk'])

# ---- End Import Property -----

##### Import Dispos #####
# [Temp] customer diso file not in S3
# status = pd.read_excel('./Dispos.xlsx')
# status = status.drop(columns=[
#     'dispid', 'userid', 'hour', 'status', 'description', 'timezone', 'timeEST',
#     '_BATCH_ID_', '_BATCH_LAST_RUN_'
# ])

# Get file from S3
dispoFile = s3.get_object(Bucket=BUCKET_NAME, Key='DUMP/DispoAdmin1.csv')
bar.next()  # Annoying to have to wait. Little status update.

# Create dataframe
status = pd.read_csv(dispoFile['Body'], encoding="ISO-8859-1", index_col=0, low_memory=False)

# Remove Junk
status = status.drop(columns=['userid', 'hour', 'status', 'description', 'timezone', 'timeEST'])

# ---- End Import Dispos -----

##### Import Stages (dispos) #####
# Get file from S3
stageFile = s3.get_object(Bucket=BUCKET_NAME, Key='DUMP/Stages1.csv')

# Create dataframe
stages = pd.read_csv(stageFile['Body'], index_col=0, low_memory=False)

# Remove Junk
stages = stages.drop(columns=[
    'userid', 'status', 'crm', 'mp', 'mid', 'sid', 'spdate', 'timestamp'
])

# We only need properties that are inventory active
stages = stages.loc[stages['pid'].isin(listOfProperties)]

# Convert stage to dispo
stages.columns = ["propertyid", 'date', 'dispo']
stages = stages[["propertyid",'dispo', 'date']]

# ---- End of Import New Stages -----

##### Process Data #####

# Merge dispos and property dispos
finaldispos = pd.concat([status, generatedDispos, stages], ignore_index=True)

# finaldispos['propertyid'] = finaldispos['propertyid'].astype(int)

# We only need properties that are inventory active
finaldispos = finaldispos.loc[finaldispos['propertyid'].isin(listOfProperties)]

# Clean dispo names. Renamed dispos to simplify
finaldispos['dispo'].replace([
  "2",
  'MASTER PROCESS CHANGED TO Welcome - SUB PROCESS CHANGED TO 21 Day Letter Sent',
  'MASTER PROCESS CHANGED TO Welcome - SUB PROCESS CHANGED TO 45 Day Letter Sent',
  'MASTER PROCESS CHANGED TO Welcome - SUB PROCESS CHANGED TO 60 Day Letter Sent',
  'MASTER PROCESS CHANGED TO Welcome - SUB PROCESS CHANGED TO New Files',
  'MASTER PROCESS CHANGED TO Welcome - SUB PROCESS CHANGED TO Pending End of Rep',
  'MASTER PROCESS CHANGED TO Welcome - SUB PROCESS CHANGED TO Pending TIQ and LPOA',
  'MASTER PROCESS CHANGED TO Welcome - SUB PROCESS CHANGED TO RRLP Pending LPOA',
  'MASTER PROCESS CHANGED TO Welcome - SUB PROCESS CHANGED TO RRLP Pending VO',
  'MASTER PROCESS CHANGED TO Welcome - SUB PROCESS CHANGED TO Under 30 days',
  'MASTER PROCESS CHANGED TO Welcome - SUB PROCESS CHANGED TO Welcome Packet Sent',
], "Welcome", inplace=True)

finaldispos['dispo'].replace([
  '3',
  '3b',
  'Estoppel New',
  'Estoppel Sent',
  'Estoppel Received',
  'Estoppel Review',
  'STATUS CHANGED TO Estoppel',
  'STATUS CHANGED TO Estoppel Received',
  'MASTER PROCESS CHANGED TO Estoppel Received - SUB PROCESS CHANGED TO Issues',
  'MASTER PROCESS CHANGED TO Estoppel Received - SUB PROCESS CHANGED TO Managers Attention',
  'MASTER PROCESS CHANGED TO Estoppel Received - SUB PROCESS CHANGED TO MF Issues',
  'MASTER PROCESS CHANGED TO Estoppel Received - SUB PROCESS CHANGED TO Pending Estoppel Review',
  'MASTER PROCESS CHANGED TO Estoppel Received - SUB PROCESS CHANGED TO Pending Resort Documents',
  'MASTER PROCESS CHANGED TO Estoppel Received - SUB PROCESS CHANGED TO Received',
  'MASTER PROCESS CHANGED TO Estoppel Received - SUB PROCESS CHANGED TO Resort Follow-up',
  'MASTER PROCESS CHANGED TO Estoppel Received - SUB PROCESS CHANGED TO Waiting on Deed',
  'MASTER PROCESS CHANGED TO Estoppel Review - SUB PROCESS CHANGED TO New',
  'MASTER PROCESS CHANGED TO Estoppel Review - SUB PROCESS CHANGED TO Resort F/U - Estoppel Dept.',
  'MASTER PROCESS CHANGED TO Estoppel Sent - SUB PROCESS CHANGED TO 14 Day Turnaround',
  'MASTER PROCESS CHANGED TO Estoppel Sent - SUB PROCESS CHANGED TO 30 Day Turnaround',
  'MASTER PROCESS CHANGED TO Estoppel Sent - SUB PROCESS CHANGED TO 45 Day Turnaround',
  'MASTER PROCESS CHANGED TO Estoppel Sent - SUB PROCESS CHANGED TO 7 Day Turnaround',
  'MASTER PROCESS CHANGED TO Estoppel Sent - SUB PROCESS CHANGED TO Estoppel Requested',
  'MASTER PROCESS CHANGED TO Estoppel Sent - SUB PROCESS CHANGED TO Issues',
  'MASTER PROCESS CHANGED TO Estoppel Sent - SUB PROCESS CHANGED TO Managers Attention'
], "Estoppel", inplace=True)

finaldispos['dispo'].replace([
  'inventory_2a', 'inventory_2b', 'STATUS CHANGED TO Inventory',
  'STATUS CHANGED TO Need Incentive Listed',
  'MASTER PROCESS CHANGED TO Inventory - SUB PROCESS CHANGED TO Inventory Pipeline',
  'MASTER PROCESS CHANGED TO Listings - SUB PROCESS CHANGED TO Liquidation Issues (3+ times)',
  'Listings',
  'Inventory'
], "Inventory", inplace=True)

finaldispos['dispo'].replace([
  'MASTER PROCESS CHANGED TO Listings - SUB PROCESS CHANGED TO New',
  'STATUS CHANGED TO Scheduled', 'STATUS CHANGED TO New',
], "Inventory - New", inplace=True)

finaldispos['dispo'].replace([
   'MASTER PROCESS CHANGED TO Listings - SUB PROCESS CHANGED TO Active',
   'STATUS CHANGED TO Active Listing', 'STATUS CHANGED TO Active',
], "Inventory - Active", inplace=True)

finaldispos['dispo'].replace([
   'MASTER PROCESS CHANGED TO Listings - SUB PROCESS CHANGED TO Unsold',
   'STATUS CHANGED TO Unsold',
], "Inventory - Unsold", inplace=True)

finaldispos['dispo'].replace([
    'MASTER PROCESS CHANGED TO Listings - SUB PROCESS CHANGED TO Scheduled',
    'Scheduled',
    'STATUS CHANGED TO Scheduled'
], "Inventory - Scheduled", inplace=True)

finaldispos['dispo'].replace([
  'STATUS CHANGED TO Ready to Relist',
  'MASTER PROCESS CHANGED TO Listings - SUB PROCESS CHANGED TO Ready to Relist',
], "Inventory - Ready to Relist", inplace=True)

finaldispos['dispo'].replace([
  '3a',
  'STATUS CHANGED TO Awaiting Buyer Info',
  'STATUS CHANGED TO PA Received',
  'STATUS CHANGED TO PA Received from Buyer',
  'STATUS CHANGED TO All Paperwork Received from Buyer',
  'STATUS CHANGED TO PA Sent to Buyer',
  'MASTER PROCESS CHANGED TO Awaiting Buyer Info - SUB PROCESS CHANGED TO 1st Warning',
  'MASTER PROCESS CHANGED TO Awaiting Buyer Info - SUB PROCESS CHANGED TO 2nd Warning',
  'MASTER PROCESS CHANGED TO Awaiting Buyer Info - SUB PROCESS CHANGED TO 3rd Warning',
  'MASTER PROCESS CHANGED TO Awaiting Buyer Info - SUB PROCESS CHANGED TO Email Sent',
  'MASTER PROCESS CHANGED TO PA Received - SUB PROCESS CHANGED TO PA Received',
  'MASTER PROCESS CHANGED TO PA Received - SUB PROCESS CHANGED TO Payment Received',
  'MASTER PROCESS CHANGED TO PA Received - SUB PROCESS CHANGED TO Pending Payment',
  'MASTER PROCESS CHANGED TO PA Received - SUB PROCESS CHANGED TO Priority',
  'MASTER PROCESS CHANGED TO PA Received - SUB PROCESS CHANGED TO Received',
  'MASTER PROCESS CHANGED TO PA Sent to Buyer - SUB PROCESS CHANGED TO 1st Warning',
  'MASTER PROCESS CHANGED TO PA Sent to Buyer - SUB PROCESS CHANGED TO 2nd Warning',
  'MASTER PROCESS CHANGED TO PA Sent to Buyer - SUB PROCESS CHANGED TO 3rd Warning',
  'MASTER PROCESS CHANGED TO PA Sent to Buyer - SUB PROCESS CHANGED TO PA sent',
  'MASTER PROCESS CHANGED TO Pending Buyer Info/Docs - SUB PROCESS CHANGED TO Pending Buyer Docs - Ema',
  'MASTER PROCESS CHANGED TO Pending Buyer Info/Docs - SUB PROCESS CHANGED TO Pending Buyer Docs - Mai',
  'Awaiting Buyer Info'
  'PA Sent to Buyer',
  'Pending Buyer Info/Docs'
  'PA Received',
], "Purchase Agreement", inplace=True)

finaldispos['dispo'].replace([
  '4a', '4b', '4c', '4d', '6a', '6b', '6c', '6d', '6e', '5', '7a', '7b',
  '7c', '7d', '7e', 'STATUS CHANGED TO Deed Prep LCS',
  'STATUS CHANGED TO Deed Prep/Doc Prep',
  'STATUS CHANGED TO Transfer Docs sent to Buyer to Execute',
  'STATUS CHANGED TO Transfer Docs sent to Client to Execute',
  'STATUS CHANGED TO Pending Deed Prep',
  'STATUS CHANGED TO Pending<br>Execution',
  'STATUS CHANGED TO Settlement Docs Execution',
  'STATUS CHANGED TO Execution', 'STATUS CHANGED TO Execution<br>Buyer',
  'STATUS CHANGED TO Execution<br>Received',
  'STATUS CHANGED TO Execution<br>Seller',
  'STATUS CHANGED TO Execution<br>Both',
  'STATUS CHANGED TO Deed Prep 01', 'STATUS CHANGED TO Deed Prep LT',
  'STATUS CHANGED TO Doc Prep', 'STATUS CHANGED TO Deedback Requested',
  'STATUS CHANGED TO Deedback', 'STATUS CHANGED TO Deed Prep',
  'MASTER PROCESS CHANGED TO Deed Prep - SUB PROCESS CHANGED TO Approved',
  'MASTER PROCESS CHANGED TO Deed Prep - SUB PROCESS CHANGED TO Chicago Title - Pending',
  'MASTER PROCESS CHANGED TO Deed Prep - SUB PROCESS CHANGED TO Chicago Title - SI Form Rcvd',
  'MASTER PROCESS CHANGED TO Deed Prep - SUB PROCESS CHANGED TO Chicago Title - Title Search',
  'MASTER PROCESS CHANGED TO Deed Prep - SUB PROCESS CHANGED TO Corrections Needed',
  'MASTER PROCESS CHANGED TO Deed Prep - SUB PROCESS CHANGED TO LCS',
  'MASTER PROCESS CHANGED TO Deed Prep - SUB PROCESS CHANGED TO LT',
  'MASTER PROCESS CHANGED TO Deed Prep - SUB PROCESS CHANGED TO Pending Approval',
  'MASTER PROCESS CHANGED TO Deed Prep - SUB PROCESS CHANGED TO Pending LCS Deed Prep',
  'MASTER PROCESS CHANGED TO Deed Prep - SUB PROCESS CHANGED TO Pending LT Deed Prep',
  'MASTER PROCESS CHANGED TO Deed Prep - SUB PROCESS CHANGED TO Resort',
  'MASTER PROCESS CHANGED TO Deedback - SUB PROCESS CHANGED TO Deedback Pipeline',
  'MASTER PROCESS CHANGED TO Deedback Requested - SUB PROCESS CHANGED TO Bluegreen',
  'MASTER PROCESS CHANGED TO Deedback Requested - SUB PROCESS CHANGED TO Hardship Letter Sent',
  'MASTER PROCESS CHANGED TO Deedback Requested - SUB PROCESS CHANGED TO Managers Attention',
  'MASTER PROCESS CHANGED TO Deedback Requested - SUB PROCESS CHANGED TO Resort Emailed',
  'MASTER PROCESS CHANGED TO Doc Prep - SUB PROCESS CHANGED TO Pending Doc Prep',
  'MASTER PROCESS CHANGED TO Doc Prep - SUB PROCESS CHANGED TO Requested Transfer Docs',
  'MASTER PROCESS CHANGED TO Doc Processing - SUB PROCESS CHANGED TO Cease & Desist with Attorney for',
  'MASTER PROCESS CHANGED TO Doc Processing - SUB PROCESS CHANGED TO Doc Review',
  'MASTER PROCESS CHANGED TO Doc Processing - SUB PROCESS CHANGED TO Docs Received/In Queue',
  'MASTER PROCESS CHANGED TO Doc Processing - SUB PROCESS CHANGED TO Pending Upload',
  'MASTER PROCESS CHANGED TO Doc Processing - SUB PROCESS CHANGED TO Prepare Cease & Desist',
  'MASTER PROCESS CHANGED TO Execution - SUB PROCESS CHANGED TO Approved',
  'MASTER PROCESS CHANGED TO Execution - SUB PROCESS CHANGED TO Bluegreen',
  'MASTER PROCESS CHANGED TO Execution - SUB PROCESS CHANGED TO Client',
  'MASTER PROCESS CHANGED TO Execution - SUB PROCESS CHANGED TO Diamond',
  'MASTER PROCESS CHANGED TO Execution - SUB PROCESS CHANGED TO Document Review',
  'MASTER PROCESS CHANGED TO Execution - SUB PROCESS CHANGED TO Execution Buyer',
  'MASTER PROCESS CHANGED TO Execution - SUB PROCESS CHANGED TO Execution Received',
  'MASTER PROCESS CHANGED TO Execution - SUB PROCESS CHANGED TO Execution Seller',
  'MASTER PROCESS CHANGED TO Execution - SUB PROCESS CHANGED TO LPOA',
  'MASTER PROCESS CHANGED TO Execution - SUB PROCESS CHANGED TO Managers Attention',
  'MASTER PROCESS CHANGED TO Execution - SUB PROCESS CHANGED TO Pending Approval',
  'MASTER PROCESS CHANGED TO Execution Received - SUB PROCESS CHANGED TO Received',
  'Deed Prep', 'Execution Received', 'ExecutionReceived',
  'Pending Deedback',
  'Deedback Requested',
  'Doc Prep',
  'Execution',
  'Recording',
  'Finalization'
], "Execution", inplace=True)

finaldispos['dispo'].replace([
  '8',
  '8a',
  '8c',
  '9',
  '9a',
  'STATUS CHANGED TO ROFR',
  'STATUS CHANGED TO RoFR',
  'STATUS CHANGED TO Waiting on Resort for finalization',
  'STATUS CHANGED TO Pending Finalization',
  'STATUS CHANGED TO Pending RoFR',
  'STATUS CHANGED TO Resort Finalization',
  'STATUS CHANGED TO Transfer Recording',
  'MASTER PROCESS CHANGED TO Finalization - SUB PROCESS CHANGED TO Bluegreen',
  'MASTER PROCESS CHANGED TO Finalization - SUB PROCESS CHANGED TO Check Requested',
  'MASTER PROCESS CHANGED TO Finalization - SUB PROCESS CHANGED TO Deedback Sent',
  'MASTER PROCESS CHANGED TO Finalization - SUB PROCESS CHANGED TO Diamond',
  'MASTER PROCESS CHANGED TO Finalization - SUB PROCESS CHANGED TO Finalization Issues',
  'MASTER PROCESS CHANGED TO Finalization - SUB PROCESS CHANGED TO Finalization Packet Sent',
  'MASTER PROCESS CHANGED TO Finalization - SUB PROCESS CHANGED TO Inventory Sent',
  'MASTER PROCESS CHANGED TO Finalization - SUB PROCESS CHANGED TO Pending Closed',
  'MASTER PROCESS CHANGED TO Finalization - SUB PROCESS CHANGED TO Pending Finalization',
  'MASTER PROCESS CHANGED TO Finalization - SUB PROCESS CHANGED TO Prepare Finalization Packet',
  'MASTER PROCESS CHANGED TO ROFR - SUB PROCESS CHANGED TO Pending ROFR',
  'MASTER PROCESS CHANGED TO ROFR - SUB PROCESS CHANGED TO ROFR Request Sent',
], "Transfered", inplace=True)

finaldispos['dispo'].replace([
  'CHECK-Check Request',
  'MASTER PROCESS CHANGED TO Recording<BR>SUB PROCESS CHANGED TO Check Requested',
  'MASTER PROCESS CHANGED TO Finalization<BR>SUB PROCESS CHANGED TO Check Requested'
  'Check Requested',
], "Check Requested", inplace=True)

finaldispos['dispo'].replace([
  '10',
  'MASTER PROCESS CHANGED TO Closed - SUB PROCESS CHANGED TO Closed'
], "Closed", inplace=True)

# ----- End of Cleaning ------

# # Bunch of random strings. Needs to be filtered to ensure properly catergorize.
# # NOTE: Only needs to run to get the list of dispos.
# listOfDispos = finaldispos.dispo.unique()
# print(listOfDispos)

# We only need the dispos for VPL
finaldispos = finaldispos.loc[finaldispos['dispo'].isin([
    "Sale Date", "Welcome", "Estoppel", "Inventory", "Purchase Agreement",
    "Hold", "Execution", "Transfered", "Doc Date", "Closed", "Today",
    "Inventory - New", "Inventory - Active", "Inventory - Unsold",
    "Inventory - Scheduled", "Inventory - Ready to Relist", "Check Requested"
])]

# Clean dispo date
finaldispos['date'].replace('', np.nan, inplace=True)
finaldispos.dropna(subset=['date'], inplace=True)
finaldispos['cleanDate'] = pd.to_datetime(finaldispos['date'], format='%Y%m%d', utc=True)

# Clean property start date
properties['cleanDate'] = pd.to_datetime(
  properties['startdate'],
  format='%Y%m%d',
  utc=True)

# If contains a closed lets mark that as true and calculated the total days.
properties['sold'] = np.where(properties['status'] == "10", True, False)

# Sort the dispos by propertyid and date so we can difference.
finaldispos = finaldispos.sort_values(by=['propertyid', 'cleanDate'])

# Find the number of days since the last dispo change
finaldispos['diff'] = finaldispos.groupby('propertyid')['cleanDate'].diff() / np.timedelta64(1, 'D')

# Make na 0
finaldispos['diff'] = finaldispos['diff'].fillna(0)

# Count the number of days by status
count = pd.crosstab(
    finaldispos['propertyid'],
    columns=finaldispos['dispo'],
    values=finaldispos['diff'],
    aggfunc='sum')

# Need to add status/sold/since update to the new dataframe.
count['Status'] = properties['status']
count['Sold'] = properties['sold']
count['Property Type'] = np.where(properties['mt_tr'] == 'TRANSFER', 'Transfer',
                         "Mortgage")
count['Since Update'] = np.where(count['Sold'] == True, 0, count['Today'])
count['Resort Family'] = properties['resortfamily'].str.strip()
count = count.drop(columns=['Today'])

# Get total days
count['totalDays'] = count.sum(axis=1)
count['Start Date'] = properties['cleanDate']

# Save the final product as a csv. Useful for testing the data is coming out.
count.to_csv('export-test.csv')

# Finally export and update domo
final = count.to_csv(header=False)

# NOTE: Sometimes a columns change. If they change it will throw an error. Need to find a more elegant solution, but if column names are removed it will mess up charts.

# listOfColumns = count.dtypes
# print(listOfColumns)

# ---- End of NOTE ----- #

# Upload to DOMO!
datasets.data_import(final_dataset_id, final)
bar.next()  # Annoying to have to wait. Little status update.
bar.finish()
print("le fin")