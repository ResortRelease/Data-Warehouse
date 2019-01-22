import pandas as pd
import numpy as np
from datetime import datetime
from progress.bar import Bar

# Import deals as contacts
dispos = pd.read_csv(
  './Exports/export-dipos.csv',
  index_col=False,
  low_memory=False,
  encoding="ISO-8859-1",
)

dispos = pd.crosstab(
    index=dispos['dealid'],
    columns=dispos['type'],
    values=dispos['type'],
    aggfunc='count')

dispos.to_csv('./Exports/count-dispos.csv')