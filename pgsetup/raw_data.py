#!/Users/weiyili/opt/anaconda3/bin/python
#cd /Users/weiyili/Desktop/Projects/user-behavior-analysis
# %%
# %pip install boto3
import json
import boto3

# %%
# Set AWS access key and secret key from json file
f = open('./pgsetup/aws-credentials.json')
credentials = json.load(f)

ACCESS_KEY = credentials["users"][0]["access_key"]
SECRET_KEY = credentials["users"][0]["secret_key"]

f.close()

# %%
s3 = boto3.client('s3', 
                  aws_access_key_id = ACCESS_KEY, 
                  aws_secret_access_key = SECRET_KEY)

s3.download_file('start-data-engg', 'data.zip', 'data.zip')