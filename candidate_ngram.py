import os, csv
from whoosh.fields import Schema, TEXT, ID
from whoosh.index import create_in, open_dir
from whoosh.query import FuzzyTerm
from whoosh.qparser import QueryParser
from whoosh.analysis import NgramTokenizer
from whoosh import scoring
import argparse


def args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--create_index", type=bool)
    parser.add_argument("--search_chemical", type=bool)
    parser.add_argument("--chem_name", type=str)
    args = parser.parse_args()
    return args

def search_chemical(index, query_string):

    query = QueryParser("name", schema=index.schema)
    with index.searcher() as searcher:
        query = query.parse(query_string)
        results = searcher.search(query, limit=5)

        matches = []
        for result in results:
            matches.append({"name": result["name"], "id": result["id"]})

        return matches


"""
NOTE:
removing spaces, punctutaions, ...
check the correct matches too (sanity check).
recall instead of match or not: if it is in top n list
n_gram_index_  rre-un it and add an extrta field to the schema for n-gram
"""

def main():

    index_dir = "./n_gram_chemicals_index2014"
    arg = args()

    if arg.create_index:
        # Define the schema with NgramTokenizer
        schema = Schema(name=TEXT(analyzer=NgramTokenizer(minsize=2, maxsize=5),stored=True),
                        id=ID(stored=True))

        # Create the index
        if not os.path.exists(index_dir):
            os.mkdir(index_dir)

        index = create_in(index_dir, schema)

        # Add documents to the index
        writer = index.writer()

        # Open the CSV file and index its contents
        csv_file = "MeSH2014.csv"
        with open(csv_file, "r", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                writer.add_document(name=row["Name"], id=row["ID"])

        # Commit the changes to the index
        writer.commit()

    if arg.search_chemical:

        # Open the index for searching
        index = open_dir(index_dir)

        # Search for a query
        matches = search_chemical(index, arg.chem_name)

        if matches:
            for i, match in enumerate(matches):
                print(f"{i+1}. {match['name']} ({match['id']})")
        else:
            print("No matches found.")

if __name__ == "__main__":
    main()
