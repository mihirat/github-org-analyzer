# github-org-analyzer

Fetch your organization's all data and save to BigQuery.

Superset is attached to visualize.

## SETUP

1. Make GCP project and enable BigQuery

2. Create BigQuery dataset to store data

## HOW TO FETCH DATA TO BigQuery
```
$ pipenv install
$ pipenv run start -t '<your personal token>' \
                   -o '<target org>' \
                   -p '<bq project to store data>' \
                   -d '<bq dataset to store>' \
                   --base-url '<optional. your github enterprise api endpoint>'
```


## HOW TO Visualize via Superset

#### Setup
```
$ cd superset
$ docker-compose up -d
$ ./init.sh # set your accounts for Web UI
```

#### Connect to BigQuery

1. Access http://localhost:8088
1. Sources > Databases > + and then fill. [ref](https://superset.incubator.apache.org/installation.html)
1. Enjoy your analysis.


Or, You can use [Data Portal](https://datastudio.google.com/u/0/navigation/reporting)


## TODO
- Verify date diff update to work
- Clean codes

## License
MIT
