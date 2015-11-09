import argparse
import csv
import sys

from elasticsearch import Elasticsearch


def query_and_dump_reults(args):
    es = Elasticsearch()
    query = '{"query":{"match_all":{}}}'
    res = es.count(index=args.index, body=query)
    nhits = res['count']
    res = es.search(index=args.index, body=query, size=nhits, _source_include=args.fields)
    fields = args.fields.split(',')
    with open(args.target, 'w', newline='') as csvfile:
        datawriter = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
        datawriter.writerow(fields)
        for item in res['hits']['hits']:
            item = item['_source']
            datawriter.writerow([item[field] for field in fields if field in fields])


def main(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("--hostname", help="ES hostname", default="localhost", type=str)
    parser.add_argument("--port", help="ES port", default="9200", type=int)
    parser.add_argument("--index", help="Name of the index", type=str)
    parser.add_argument("--fields", help="A list of fields to extract and return", type=str)
    parser.add_argument("--target", help="Output csv file path", type=str)
    parser.set_defaults(func=query_and_dump_reults)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main(sys.argv[1:])
