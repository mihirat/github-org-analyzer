from google.cloud import bigquery
from pandas import DataFrame as df
from lib.util.parse_url import extract_org, extract_repo
from datetime import datetime

TABLE_SCHEMA = [
    bigquery.SchemaField(name='org', field_type=bigquery.enums.SqlTypeNames.STRING),
    bigquery.SchemaField(name='repo', field_type=bigquery.enums.SqlTypeNames.STRING),
    bigquery.SchemaField(name='number', field_type=bigquery.enums.SqlTypeNames.INT64),
    bigquery.SchemaField(name='user', field_type=bigquery.enums.SqlTypeNames.STRING),
    bigquery.SchemaField(name='title', field_type=bigquery.enums.SqlTypeNames.STRING),
    bigquery.SchemaField(name='body', field_type=bigquery.enums.SqlTypeNames.STRING),
    bigquery.SchemaField(name='additions', field_type=bigquery.enums.SqlTypeNames.INT64),
    bigquery.SchemaField(name='deletions', field_type=bigquery.enums.SqlTypeNames.INT64),
    bigquery.SchemaField(name='labels', field_type=bigquery.enums.SqlTypeNames.STRING),
    bigquery.SchemaField(name='is_merged', field_type=bigquery.enums.SqlTypeNames.BOOLEAN),
    bigquery.SchemaField(name='closed_at', field_type=bigquery.enums.SqlTypeNames.TIMESTAMP),
    bigquery.SchemaField(name='diff_url', field_type=bigquery.enums.SqlTypeNames.STRING),
]


class Pulls:
    def __init__(self, pulls, begin, end):
        if isinstance(begin, datetime):
            pulls = [pull for pull in pulls if pull.closed_at and pull.closed_at >= begin]
        if isinstance(end, datetime):
            pulls = [pull for pull in pulls if pull.created_at <= end]

        data = [[extract_org(pull.html_url), extract_repo(pull.html_url), int(pull.number), pull.user.login,
                 pull.title, pull.body, ','.join([l.name for l in pull.labels if l.name is not None]),
                 int(pull.additions), int(pull.deletions), pull.merged, pull.closed_at, pull.diff_url] for pull in
                pulls]

        self.items = pulls
        self.pulls = df(data, columns=['org', 'repo', 'number', 'user', 'title', 'body', 'labels'
            , 'additions', 'deletions', 'is_merged', 'closed_at', 'diff_url'])

    def append(self, another):
        self.pulls = self.pulls.append(another.pulls, ignore_index=True)

    def to_gbq(self, project, dataset, org):
        if self.pulls.empty:
            return
        client = bigquery.Client(project=project)
        job_config = bigquery.LoadJobConfig(schema=TABLE_SCHEMA)
        job = client.load_table_from_dataframe(self.pulls, destination=f'{project}.{dataset}.{org}_pulls',
                                               job_config=job_config, location='US')
        job.result()
