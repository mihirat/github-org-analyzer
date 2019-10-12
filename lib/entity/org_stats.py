from lib.entity.pulls import Pulls
from lib.entity.repos import Repos
from lib.entity.pull_comments import PullComments
from lib.entity.issue_comments import IssueComments
from lib.entity.issues import Issues
import re


class OrgStats:
    def __init__(self):
        self.repos = None
        self.pulls = None
        self.pull_comments = None
        self.issues = None
        self.issue_comments = None

    def append(self, another):
        if isinstance(another, Pulls):
            if self.pulls is not None:
                self.pulls.append(another)
            else:
                self.pulls = another
        if isinstance(another, Repos):
            if self.repos is not None:
                self.repos.append(another)
            else:
                self.repos = another
        if isinstance(another, PullComments):
            if self.pull_comments is not None:
                self.pull_comments.append(another)
            else:
                self.pull_comments = another
        if isinstance(another, Issues):
            if self.issues is not None:
                self.issues.append(another)
            else:
                self.issues = another
        if isinstance(another, IssueComments):
            if self.issue_comments is not None:
                self.issue_comments.append(another)
            else:
                self.issue_comments = another

    def to_gbq(self, project, dataset, org):
        safe_org = re.sub('[^0-9a-zA-Z]+', '', org)
        if self.issues is not None:
            self.issues.to_gbq(project, dataset, safe_org)
        if self.issue_comments is not None:
            self.issue_comments.to_gbq(project, dataset, safe_org)
        if self.pulls is not None:
            self.pulls.to_gbq(project, dataset, safe_org)
        if self.pull_comments is not None:
            self.pull_comments.to_gbq(project, dataset, safe_org)
        if self.repos is not None:
            self.repos.to_gbq(project, dataset, safe_org)
