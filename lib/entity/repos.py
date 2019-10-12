from google.cloud import bigquery
from pandas import DataFrame as df
from lib.util.parse_url import extract_org, extract_repo
from datetime import datetime


TABLE_SCHEMA = [
    bigquery.SchemaField(name='org', field_type=bigquery.enums.SqlTypeNames.STRING),
    bigquery.SchemaField(name='repo', field_type=bigquery.enums.SqlTypeNames.STRING),
    bigquery.SchemaField(name='language', field_type=bigquery.enums.SqlTypeNames.STRING),
    bigquery.SchemaField(name='created_at', field_type=bigquery.enums.SqlTypeNames.TIMESTAMP),
]


class Repos:
    def __init__(self, repos, begin, end):
        if isinstance(begin, datetime):
            repos = [repo for repo in repos if repo.updated_at >= begin]
        if isinstance(end, datetime):
            repos = [repo for repo in repos if repo.created_at <= end]
        data = [[extract_org(repo.html_url),
                 extract_repo(repo.html_url),
                 repo.language,
                 repo.created_at,
                 ] for repo in repos]

        self.items = repos
        self.repos = df(data, columns=['org', 'repo', 'language', 'created_at'])

    def append(self, another):
        self.repos = self.repos.append(another.repos, ignore_index=True)

    def to_gbq(self, project, dataset, org):
        if self.repos.empty:
            return
        client = bigquery.Client(project=project)
        job_config = bigquery.LoadJobConfig(schema=TABLE_SCHEMA)
        job = client.load_table_from_dataframe(self.repos, f'{project}.{dataset}.{org}_repos', job_config=job_config, location='US')
        job.result()
