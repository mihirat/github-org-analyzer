from lib.service.restore import RestoreService
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--base-url', default='https://api.github.com', help='for GHE, https://your-ghe-domain/api/v3')
    parser.add_argument('-t', '--token', help='print debug messages')
    parser.add_argument('-o', '--org', help='api base endpoint')
    parser.add_argument('-d', '--bq-dataset', help='api base endpoint')
    parser.add_argument('-p', '--bq-project', help='api base endpoint')
    args = parser.parse_args()

    restore_service = RestoreService(args.token, args.base_url)
    restore_service.restore_all(org=args.org, bq_project=args.bq_project, bq_dataset=args.bq_dataset)
