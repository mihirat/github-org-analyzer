from google.cloud import bigquery
from pandas import DataFrame as df
import pandas as pd
from lib.util.parse_url import extract_org, extract_repo
from datetime import datetime


TABLE_SCHEMA = [
    bigquery.SchemaField(name='org', field_type=bigquery.enums.SqlTypeNames.STRING),
    bigquery.SchemaField(name='repo', field_type=bigquery.enums.SqlTypeNames.STRING),
    bigquery.SchemaField(name='number', field_type=bigquery.enums.SqlTypeNames.INT64),
    bigquery.SchemaField(name='user', field_type=bigquery.enums.SqlTypeNames.STRING),
    bigquery.SchemaField(name='comment', field_type=bigquery.enums.SqlTypeNames.STRING),
    bigquery.SchemaField(name='updated_at', field_type=bigquery.enums.SqlTypeNames.TIMESTAMP),
    bigquery.SchemaField(name='commit_id', field_type=bigquery.enums.SqlTypeNames.STRING),
    bigquery.SchemaField(name='path', field_type=bigquery.enums.SqlTypeNames.STRING),
]


class PullComments:
    def __init__(self, df_issue_comments, df_review_comments):
        self.pull_comments = pd.concat([df_issue_comments, df_review_comments], sort=False)

    def append(self, another):
        self.pull_comments = self.pull_comments.append(another.pull_comments, ignore_index=True)

    def to_gbq(self, project, dataset, org):
        if self.pull_comments.empty:
            return
        self.pull_comments['updated_at'] = pd.to_datetime(self.pull_comments['updated_at'],unit='s')
        client = bigquery.Client(project=project)
        job_config = bigquery.LoadJobConfig(schema=TABLE_SCHEMA)
        job = client.load_table_from_dataframe(self.pull_comments, f'{project}.{dataset}.{org}_pull_comments', job_config=job_config, location='US')
        job.result()


class PullIssueComments:
    def __init__(self, pull, comments, begin, end):
        if isinstance(begin, datetime):
            comments = [comment for comment in comments if comment.created_at >= begin]
        if isinstance(end, datetime):
            comments = [comment for comment in comments if comment.created_at <= end]

        data = [[extract_org(pull.html_url), extract_repo(pull.html_url), int(pull.number), comment.user.login,
                 comment.body, comment.updated_at
                 ] for comment in comments]

        self.items = comments
        self.issue_comments = df(data, columns=['org', 'repo', 'number', 'user', 'comment',
                                                'updated_at'])


class PullReviewComments:
    def __init__(self, pull, comments, end):
        if isinstance(end, datetime):
            comments = [comment for comment in comments if comment.created_at <= end]

        data = [[extract_org(pull.html_url), extract_repo(pull.html_url), int(pull.number), comment.user.login,
                 comment.body, comment.updated_at, comment.commit_id, comment.path
                 ] for comment in comments]

        self.items = comments
        self.review_comments = df(data, columns=['org', 'repo', 'number', 'user', 'comment',
                                                 'updated_at', 'commit_id', 'path'])
