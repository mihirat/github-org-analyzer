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
    bigquery.SchemaField(name='labels', field_type=bigquery.enums.SqlTypeNames.STRING),
    bigquery.SchemaField(name='assignees', field_type=bigquery.enums.SqlTypeNames.STRING),
    bigquery.SchemaField(name='is_pr', field_type=bigquery.enums.SqlTypeNames.BOOLEAN),
    bigquery.SchemaField(name='created_at', field_type=bigquery.enums.SqlTypeNames.TIMESTAMP),
    bigquery.SchemaField(name='closed_at', field_type=bigquery.enums.SqlTypeNames.TIMESTAMP),
]


class Issues:

    def __init__(self, issues, begin, end):
        if isinstance(begin, datetime):
            issues = [issue for issue in issues if issue.closed_at and issue.closed_at >= begin]
        if isinstance(end, datetime):
            issues = [issue for issue in issues if issue.created_at <= end]

        data = [[extract_org(issue.html_url), extract_repo(issue.html_url), int(issue.number), issue.user.login,
                 issue.title, issue.body, ','.join([l.name for l in issue.labels if l.name is not None]),
                 ','.join([a.name for a in issue.assignees if a.name is not None]),
                 issue.pull_request is not None, issue.created_at, issue.closed_at] for issue in issues]

        self.items = issues
        self.issues = df(data, columns=['org', 'repo', 'number', 'user', 'title', 'body', 'labels', 'assignees',
                                        'is_pr', 'created_at', 'closed_at'])

    def append(self, another):
        self.issues = self.issues.append(another.issues, ignore_index=True)

    def to_gbq(self, project, dataset, org):
        if self.issues.empty:
            return
        client = bigquery.Client(project=project)
        job_config = bigquery.LoadJobConfig(schema=TABLE_SCHEMA)
        job = client.load_table_from_dataframe(dataframe=self.issues, destination=f'{project}.{dataset}.{org}_issues',
                                               job_config=job_config, location='US')
        job.result()
