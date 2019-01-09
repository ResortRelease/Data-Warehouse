import re
import pandas as pd
import base64

us_zip = pd.read_csv(
    './Imports/US.csv',
    index_col=False,
    low_memory=False
)

def fronter_name(name=None, full=False): 
  pattern = re.compile("(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)")
  if pd.isna(name) == False:
    return name
  else:
    return "Taylor" 

def clean_client_name(x):
  if pd.isna(x) == False:
    x = x.lstrip()
    x = x.rstrip()
    x = re.sub(r"\(.*\)","", x) # Remove "(anything)"
    x = re.sub(r"[^a-zA-Z0-9 -]","", x) # Remove weird emoji stuff
    string_array = x.split(" ") # Create array of strings
    name_length = len(string_array) # Count the length of array
    fullname = ''
    if (name_length == 1):
      fullname = string_array[0]
    elif (name_length == 2):
      fullname = string_array[0] + " " + string_array[1]
    elif (name_length == 3):
      fullname = string_array[0] + " " + string_array[2]
    elif (name_length > 4):
      fullname = string_array[0] + " " + string_array[name_length - 1]
    else:
      fullname = "there !"

    cleaned = fullname.title()
    return cleaned
  else:
    return "there"

def clean_number(number):
  pattern = re.compile("^(\+\d{1,2}\s)?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}$")
  if pd.isna(number) == False:
    phone_exists = bool(pattern.match(number))
    if phone_exists == True:
      # Only return the numbers
      clean = int(''.join(ele for ele in number if ele.isdigit()))
      return clean
    else:
      return ""
  else:
    return ""

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

def clean_status(x):
  if pd.isna(x) == False:
    cleaned = re.findall("^[a-zA-Z09]*[^-]", x)[0]
    return cleaned

# Resort names / generic cleaning
def clean_name(x):
  if pd.isna(x) == False:
    return x.title()

def clean_campaign(x):
  if pd.isna(x) == False:
    pattern = re.compile("^[\d]*$")
    is_number = bool(pattern.match(x))
    if is_number == False:
      return x.lower()
    else:
      return ''
  else:
    return ''

def clean_status(x):
  if pd.isna(x) == False:
    cleaned = re.findall("^[a-zA-Z09]*[^-]", x)[0]
    return cleaned

def clean_name(x):
  if pd.isna(x) == False:
    return x.lower()

def format_date(date):
  if pd.isna(date) == False:
    string_date = str(date)
    month = string_date[4:6]
    day = string_date[6:8]
    year = string_date[0:4]
    return (f'{month}/{day}/{year}')
  else:
    return False

def decide_medium(x):
  x = x.lower()
  if 'facebook' in x:
    return "Facebook"
  elif 'call' in x:
    return "Inbound Call"
  else:
    return "Organic / Untracked"

def decide_source(row):
  has_heard = len(row.hearduson) > 0
  has_source = len(row.utm_source) > 0
  has_subsource = len(row.SubSource) > 0
  # Default to what the user says to the sales person on the phone
  if has_heard == True:
    if any(x in row.hearduson for x in ['RADIO','Radio','radio']):
      return 'Radio'
    elif any(x in row.hearduson for x in ["ONLINE SEARCH", "Online Search"]):
      return 'PPC'
    elif any(x in row.hearduson for x in ["FACEBOOK", "Facebook", "facebook"]):
      return 'Facebook'
    elif any(x in row.hearduson for x in ["TV", "Tv", "tv"]):
      return 'TV'
    else:
      return 'Other'
  elif any(x in row.utm_campaign for x in ["chatbot"]):
      return 'Facebook Messenger'
  # If contact has a source attach it
  elif has_source == True:
    if any(x in row.utm_source for x in ["facebook.com","facebook","facebookfronter","facebookfb","facebook-fb","facebook-ig","facebooklaura"]):
      return 'Facebook'
    elif any(x in row.utm_source for x in ["google","bing","youtube","msn","adroll","linkedin"]):
      return 'PPC'
    elif any(x in row.utm_source for x in ["facebookmsg","facebook-messenger"]):
      return 'Social'
    elif any(x in row.utm_source for x in ["bbb"]):
      return 'BBB'
    else:
      return 'Other'
  # If contact has a source attach it
  elif has_subsource == True:
    SubSource = row.SubSource.lower()
    # mark levin, laura ingraham, michael savage
    if any(x in SubSource for x in ["facebook inbound call"]):
      return 'FB Call'
    elif any(x in SubSource for x in ["refer"]):
      return 'Referral'
    elif any(x in SubSource for x in ["facebook", "fb"]):
      return 'Facebook'
    elif any(x in SubSource for x in ["online search", "google","bing","youtube","msn","adroll","linkedin", 'timeshareexitteam', 'buyology iq', 'keywordcampaign']):
      return 'PPC'
    elif any(x in SubSource for x in ["online search"]):
      return 'PPC Call'
    elif any(x in SubSource for x in ["tv"]):
      return 'TV'
    elif any(x in SubSource for x in ["bbb"]):
      return 'BBB'
    elif any(x in SubSource for x in ["radio"]):
      return 'Radio'
    else:
      return row['medium']
  else:
    return row['medium']

def reassign(row):
  # User reapplied we need to find the webhook and add the source.
  reapplied = True if row.webhook < 0 else False 
  if reapplied == True:
    return row
  else:
    return row

def stringToBase64(s):
  base = base64.b64encode(s.encode('utf-8'))
  return base.decode("utf-8")

def add_county(zip):
  if pd.isna(zip) == False:
    pattern = re.compile("^[\d]{5}")
    is_number = bool(pattern.match(zip))
    if is_number == True:
      # find zip in us_zip
      row = us_zip.loc[us_zip['zip'].isin([zip])]
      if len(row['county'].values) > 0:
        # Return county
        return row['county'].values[0]

def was_sold(val):
  if pd.isna(val) == False:
    return 1
  else:
    return 0