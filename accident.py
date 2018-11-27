import pandas as pd

contacts = pd.read_csv(
    './Properties1.csv',
    index_col=0,
    low_memory=False,
    encoding="ISO-8859-1",
)

accident = pd.read_csv(
    './accident.csv',
    index_col=False,
    low_memory=False,
    encoding="ISO-8859-1",
)

unique_email = accident['Email'].unique().tolist()

contacts = contacts.loc[contacts['email'].isin(unique_email)]
unique_contacts = contacts['email'].unique().tolist()
contacts = contacts.loc[contacts['email'].isin(unique_contacts)]

print(len(unique_contacts))

contacts = contacts[['email', 'phone1', 'client', 'phone2', 'status', 'lastdispo']]
contacts.to_csv('export-sorry.csv')