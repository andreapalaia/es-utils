import argparse
import csv
import sys

import progressbar
from elasticsearch import Elasticsearch, helpers


def get_var(input_dict, accessor_string):
    """Gets data from a dictionary using a dotted accessor-string"""
    current_data = input_dict
    for chunk in accessor_string.split('.'):
        current_data = current_data.get(chunk, {})
    return current_data


def query_and_dump_reults(args):
    es = Elasticsearch([args.hostname + ':' + str(args.port)])

    query = '{"query":{"match_all":{}}}'
    if args.query is not None:
        query = args.query

    doc_type = None
    if args.doc_type is not None:
        doc_type = args.doc_type

    target = "output.csv"
    if args.target is not None:
        target = args.target

    res = es.count(index=args.index, body=query)
    nhits = res['count']

    counter = 0
    bar = progressbar.ProgressBar(max_value=nhits)

    res = helpers.scan(es, index=args.index, query=query, doc_type=doc_type)
    fields = args.fields.split(',')
    with open(target, 'w') as csvfile:
        datawriter = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
        datawriter.writerow(fields)
        for item in res:
            item = item['_source']
            datawriter.writerow([get_var(item, field) for field in fields])

            counter += 1
            bar.update(counter)
        bar.finish()


def main(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("--hostname", help="ES hostname", default="localhost", type=str)
    parser.add_argument("--port", help="ES port", default="9200", type=int)
    parser.add_argument("--index", help="Name of the index", type=str)
    parser.add_argument("--query", dest="query", help="Query DSL", type=str)
    parser.add_argument("--doc_type", dest="doc_type", help="Document type", type=str)
    parser.add_argument("--fields", help="A list of fields to extract and return", type=str, required=True)
    parser.add_argument("--target", help="Output csv file path", type=str)
    parser.set_defaults(func=query_and_dump_reults)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main(sys.argv[1:])
