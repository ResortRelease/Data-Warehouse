# Data-Warehouse

## Install Requirements
```cd domo-scripts```

```pip install awscli```

Then fill out the AWS credentials. Config file does not have aws info.

Then install the rest

```brew install pipenv```

```pipenv install```

3.6 is required. Mac has an issue with installing 3.6
```CFLAGS="-I$(xcrun --show-sdk-path)/usr/include" pyenv install -v 3.6.0```

```secrets.py``` (deprecated domo) is not included in this repo because it has sensitive information. Please request this file from Kyle Pierce. 




