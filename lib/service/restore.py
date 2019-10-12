from datetime import datetime

import github

from lib.entity.org_stats import OrgStats
from lib.entity.repos import Repos
from lib.entity.pulls import Pulls
from lib.entity.issues import Issues
from lib.entity.pull_comments import PullReviewComments, PullIssueComments, PullComments
from lib.entity.issue_comments import IssueComments


class RestoreService:
    def __init__(self, token, base_url):
        self.gh = github.Github(token, base_url=base_url)

    @staticmethod
    def get_comments_of_issue(issue):
        return IssueComments(issue, issue.get_comments())

    @staticmethod
    def get_comments_of_pull(pull, begin=None, end=None):
        since = begin if isinstance(begin, datetime) else github.GithubObject.NotSet
        prc = PullReviewComments(pull, pull.get_review_comments(since), end)
        pic = PullIssueComments(pull, pull.get_issue_comments(), begin, end)
        return PullComments(prc.review_comments, pic.issue_comments)

    @staticmethod
    def get_pulls_of_repo(repo, begin=None, end=None):
        return Pulls(repo.get_pulls(state='all'), begin, end)

    @staticmethod
    def get_issues_of_repo(repo, begin=None, end=None):
        return Issues(repo.get_issues(state='all'), begin, end)

    def get_repos_of_org(self, org, begin=None, end=None):
        gh_org = self.gh.get_organization(org)
        return Repos(gh_org.get_repos(), begin, end)

    def restore_all(self, org, begin=None, end=None, bq_project='', bq_dataset=''):
        org_stats = OrgStats()
        repos = self.get_repos_of_org(org, begin=begin, end=end)
        org_stats.append(repos)

        for repo in repos.items:
            pulls = self.get_pulls_of_repo(repo, begin=begin, end=end)
            org_stats.append(pulls)

            for pull in pulls.items:
                pull_comments = self.get_comments_of_pull(pull, begin=begin, end=end)
                org_stats.append(pull_comments)

            issues = self.get_issues_of_repo(repo, begin=begin, end=end)
            org_stats.append(issues)
            for issue in issues.items:
                issue_comments = self.get_comments_of_issue(issue)
                org_stats.append(issue_comments)

        org_stats.to_gbq(project=bq_project, dataset=bq_dataset, org=org)
