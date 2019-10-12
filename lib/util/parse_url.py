import re


def extract_org(url):
    return extract_key(url, 'org')


def extract_repo(url):
    return extract_key(url, 'repo')


def extract_key(url, key):
    r_str = 'https://(?P<domain>[a-zA-Z0-9_.\-]+)/(?P<org>[a-zA-Z0-9_.\-]+)/(?P<repo>[a-zA-Z0-9_.\-]+)(?P<drop>.*)'
    return re.match(r_str, url).group(key)
