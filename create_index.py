import os
import csv
import re
from whoosh.index import create_in
from whoosh.fields import TEXT, ID, Schema
from whoosh.qparser import QueryParser
# from whoosh.analysis import Filter
import argparse



def args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--csv_file", help="path to csv file")
    parser.add_argument("-i", "--index_dir", help="path to index directory")
    args = parser.parse_args()
    return args


def preprocess(text):
    # remove thhe spaces and punctutaions
    # print(text)
    text = re.sub(r'\s+', '', text)
    text = re.sub(r'[^\w\s]', '', text)
    # print(text + "\n")
    return text

def add_documents_to_index(index, csv_file):
    # Open the index writer
    writer = index.writer()

    # Open the CSV file and index its contents
    with open(csv_file, "r", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            writer.add_document(name=preprocess(row["Name"]), id=row["ID"])

    # Commit the changes to the index
    writer.commit()


def main():
    arg = args()
    schema = Schema(name=TEXT(stored=True), id=ID(stored=True))
    index_dir = arg.index_dir
    if not os.path.exists(index_dir):
        os.mkdir(index_dir)
    index = create_in(index_dir, schema)
    add_documents_to_index(index, arg.csv_file)


if __name__ == "__main__":
    main()
