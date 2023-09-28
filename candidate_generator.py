from whoosh.index import open_dir
from whoosh.qparser import QueryParser
import argparse

def args():
    parser = argparse.ArgumentParser()
    parser.add_argument("chemical_name", help="name of chemical to search for")
    parser.add_argument("-i", "--index_dir", help="path to index directory", default="chemicals_index")
    args = parser.parse_args()
    return args


def search_chemical(index, query_string):

    with index.searcher() as searcher:
        query = QueryParser("name", schema=index.schema).parse(query_string)
        results = searcher.search(query, limit=None)
        
        matches = []
        for result in results:
            matches.append({"name": result["name"], "id": result["id"]})
        
        return matches

def main():

    arg = args()
    index_dir = arg.index_dir
    chemical_name = arg.chemical_name

    index = open_dir(index_dir)
    matches = search_chemical(index, chemical_name)

    if matches:
        for i, match in enumerate(matches):
            print(f"{i+1}. {match['name']} ({match['id']})")
    else:
        print("No matches found.")


if __name__ == "__main__":
    main()
