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

def employee (dispo):
  if pd.isna(dispo) == False:
    if any(x in dispo for x in ['apset','ows','lms','apconfirmed','gotrid','cbk','aprschld','cant','email','notenough','nas','awc','mbf','clsrnoap','nit','moveto','dnc','custnoap','dna','custnoap_rsch','wrong','moveto','asm','mtg not qualified (no fraud)','notwknumb','spanish','30 days cbk','60 days cbk','90 days cbk','allbusy']):
      return "Fronter"
    elif any(x in dispo for x in ['nosalecb','sale','ccbk''nosale','sentvo-sent to vo','24follow','cancelsave']):
      return "Closer"
    else:
      return "Other"

def sentiment (dispo):
  if pd.isna(dispo) == False:
    if any(x in dispo for x in ['ows','gotrid','cant','notenough','awc','clsrnoap','moveto','custnoap','custnoap_rsch','moveto','mtg not qualified (no fraud)','spanish','nosalecb','nosale','cancelsave']):
      return "Negative"
    elif any(x in dispo for x in ['apset','apconfirmed','aprschld','sale','sentvo-sent to vo']):
      return "Postive"
    else:
      return "Nutral"
  
def contact_type (dispo):
  if pd.isna(dispo) == False:
    if any(x in dispo for x in ['lms','cbk','email','nas','mbf','nit','dnc','dna','wrong','asm','notwknumb','30 days cbk','60 days cbk','90 days cbk','allbusy','ccbk','24follow']):
      return 'Not Contacted'
    elif any(x in dispo for x in ['apset','apconfirmed','aprschld','ows','gotrid','cant','notenough','awc','clsrnoap','moveto','custnoap','custnoap_rsch','moveto','mtg not qualified (no fraud)','spanish','sale','sentvo','nosalecb','nosale','cancelsave']):
      return 'Contacted'
    else:
      return 'Admin'

# Import deals as contacts
dispos = s3.get_object(Bucket=BUCKET_NAME, Key='DUMP/Dispo1.csv')
bar.next()
dispos = pd.read_csv(
    dispos['Body'],
    # './Imports/Dispo1.csv',
    index_col=False,
    low_memory=False,
    encoding="ISO-8859-1",
)
bar.next()
dispos = dispos.drop(columns=['description', 'qareport2', 'fronter', 'status', 'timezone', 'timeEST'])

dispos['dispo'] = dispos['dispo'].str.lower()
dispos['postion'] = dispos['dispo'].apply(employee)
dispos['sentiment'] = dispos['dispo'].apply(sentiment)
dispos['type'] = dispos['dispo'].apply(contact_type)
dispos['date'] = dispos['date'].apply(rr_fun.format_date)

bar.next()
dispos.to_csv('./Exports/export-dipos.csv', index=False)
bar.finish()