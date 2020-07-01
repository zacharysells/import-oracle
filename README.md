# import-oracle

## Initialize cdk.json and variables
Minimally fill in cdk.json as follows
```
{
  "context": {
    "oracle-rds-username": "admin",
    "oracle-rds-password": "password",
    "public-ipv4-cidr-allowed": "1.1.1.1/32"
  }
}
```



## Deploy stack
```
pip install -r requirements.txt
cdk diff
cdk deploy
```

## Set environment variables
```
export DB_USER="admin"
export DB_PASSWORD="password"
export DB_HOST="something.rds.amazonaws.com" # Get this value from the deployed RDS resource
```

## Run import-oracle script
```
# Example command
$ python import-oracle.py SCL test_01 input.txt --header --empty-target

# Usage info
$ python import-oracle.py --help
```
