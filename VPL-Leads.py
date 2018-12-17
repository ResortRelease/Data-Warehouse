import pandas as pd
import numpy as np
from datetime import datetime
from progress.bar import Bar

# Secure access to domo
import secret

# General functions for RR
import rr_fun

bar = Bar('Processing', max=3)
bar.start()

contacts = pd.read_csv(
    './Imports/vpl-form-2.csv',
    index_col=False,
    low_memory=False,
    encoding="ISO-8859-1",
)

bar.next()

# Remove all columns except the following
contacts = contacts[['full_name', 'phone_number', 'email']]

# Clean phone number
contacts['phone_number'] = contacts['phone_number'].apply(rr_fun.clean_number)

# Clean emails
contacts['email'] = contacts['email'].apply(rr_fun.clean_email)

# Drop "Not a valid email"
contacts = contacts.loc[-contacts['email'].isin(["Not a valid email"])]

# Clean the names of clients and resorts
contacts['full_name'] = contacts['full_name'].apply(rr_fun.clean_client_name)
contacts['First Name'], contacts['Last Name'] = contacts['full_name'].str.split(' ', 1).str

bar.next()

# # Remove some extra fields
# contacts = contacts.drop(columns=['full_name', 'First Name', 'Last Name', 'phone_number', 'email'])

# Save the final product as a csv.
contacts.to_csv('export-vpl.csv')

bar.next()

bar.finish()

72183,73012,71527,71084,73434,70718,72607