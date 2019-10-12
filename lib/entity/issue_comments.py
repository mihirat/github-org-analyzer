from google.cloud import bigquery
from pandas import DataFrame as df
from lib.util.parse_url import extract_org, extract_repo
from datetime import datetime

TABLE_SCHEMA = [
    bigquery.SchemaField(name='org', field_type=bigquery.enums.SqlTypeNames.STRING),
    bigquery.SchemaField(name='repo', field_type=bigquery.enums.SqlTypeNames.STRING),
    bigquery.SchemaField(name='number', field_type=bigquery.enums.SqlTypeNames.INT64),
    bigquery.SchemaField(name='user', field_type=bigquery.enums.SqlTypeNames.STRING),
    bigquery.SchemaField(name='comment', field_type=bigquery.enums.SqlTypeNames.STRING),
    bigquery.SchemaField(name='updated_at', field_type=bigquery.enums.SqlTypeNames.TIMESTAMP),
]


class IssueComments:
    def __init__(self, issue, comments):
        data = [[extract_org(issue.html_url), extract_repo(issue.html_url), int(issue.number), comment.user.login,
                 comment.body, comment.updated_at
                 ] for comment in comments]

        self.items = comments
        self.issue_comments = df(data, columns=['org', 'repo', 'number', 'user', 'comment',
                                                'updated_at'])

    def append(self, another):
        self.issue_comments = self.issue_comments.append(another.issue_comments, ignore_index=True)

    def to_gbq(self, project, dataset, org):
        if self.issue_comments.empty:
            return
        client = bigquery.Client(project=project)
        job_config = bigquery.LoadJobConfig(schema=TABLE_SCHEMA)
        job = client.load_table_from_dataframe(self.issue_comments, f'{project}.{dataset}.{org}_issue_comments', job_config=job_config, location='US')
        job.result()
