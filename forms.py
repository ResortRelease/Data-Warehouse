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

forms = pd.read_csv(
    './Forms1.csv',
    index_col=False,
    low_memory=False
)

def clean_fee(fee, matching):
  if pd.isna(fee) == False:
    only_int = fee.split(matching) # Create array of strings
    return only_int[0]

forms['mfee'] = forms['mfee'].apply(clean_fee, matching="-")

# # Remove '$' and other junk
forms['mfee'] = forms['mfee'].replace('[^0-9.]', '', regex=True)

forms['mfee'] = forms['mfee'].apply(clean_fee, matching=".")

# forms['mfee'] = forms['mfee'].astype(float)

# forms = forms.groupby('dealid').agg({
#                             'mfee':', '.join})

forms.to_csv('export-forms.csv')

