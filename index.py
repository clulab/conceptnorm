from whoosh.index import create_in
from whoosh.index import open_dir
from whoosh.fields import *
from whoosh.qparser import QueryParser
import pandas as pd
import os

def create_schema_and_index(index_path = "index", new_index = False):
    if not new_index:
        index = open_dir(index_path)
        return index
    
    schema = Schema(CUI=ID(stored=True), STR=TEXT(stored=True))
    if not os.path.exists(index_path):
        os.mkdir(index_path)
    index = create_in(index_path, schema)
    write_index(index, "MRCONSO.RRF")
    return index

def write_index(index, mrconso_path):
    writer = index.writer()
    print("Writing index...")
    i = 0

    with open(mrconso_path, "r") as f:
        for line in f:
            if i % 1000000 == 0:
                print(f"Processed {i} lines")
            i += 1
            fields = line.split("|")
            if fields[1] != "ENG":
                continue
            if fields[11] != "SNOMEDCT_US" and fields[11] != "MSH":
                continue
            CUI, STR = fields[0], fields[14]
            writer.add_document(CUI=CUI, STR=STR)
    writer.commit()

    print("Done")

def search(index, query_string):
    results = []
    with index.searcher() as searcher:
        query = QueryParser("STR", index.schema).parse(query_string)
        res = searcher.search(query)
        for hit in res:
            results.append(dict(hit))
    return results

def main():
    index = create_schema_and_index(new_index = True)
    query_string = "131"
    results = search(index, query_string)
    for result in results:
        print(result)
    

if __name__ == "__main__":
    main()

