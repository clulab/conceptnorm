import os, csv, re
import pandas as pd
from whoosh.fields import Schema, TEXT, ID
from whoosh.index import create_in, open_dir
# from whoosh.query import FuzzyTerm
from whoosh.qparser import QueryParser, FuzzyTermPlugin
from whoosh.analysis import NgramFilter, LowercaseFilter, RegexTokenizer, NgramTokenizer
from whoosh import scoring
import argparse


no_matches = [] # no returned matches
wrong_matches = [] # returned matches, but none of them are correct
incorrect_matches = [] # returned matches, but the first one is not correct

def args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-ci", "--create_index", type=bool, default=False)
    parser.add_argument("-ms", "--mesh_search", type=bool, default=False)
    parser.add_argument("-fz", "--fuzzy_distance", type=int, default=1)
    parser.add_argument("-k", "--top_k", type=int, default=50)
    parser.add_argument("-max", "--max_ngram", type=int, default=5)
    parser.add_argument("-min", "--min_ngram", type=int, default=2)
    parser.add_argument("-a", "--annotation_file", help="path to annotation file", default="chemical_annotations.csv")
    parser.add_argument("-d", "--details_output_file", help="path to details.txt output file", type=str, default="results/chemical_matches_details2014.txt")
    parser.add_argument("-x", "--excel_output_file", help="path to excel output file", type=str, default="results/chemical_matches2014.xlsx")
    args = parser.parse_args()
    return args

def search_chemical(query,
                    index,
                    query_string,
                    fuzzy=False,
                    fuzzy_distance=1,
                    limit=None):

    if fuzzy:
        query.add_plugin(FuzzyTermPlugin())
        query_string = query_string+f"~{fuzzy_distance}"

    with index.searcher() as searcher:
        query = query.parse(query_string)
        results = searcher.search(query, limit=limit)

        matches = []
        for result in results:
            matches.append(
                    (result["name"], result["id"])
            )
        # print(f"matches:  {matches} \n")
        return matches

def preprocess(text):
    # remove thhe spaces and punctutaions
    # print(text)
    text = re.sub(r'\s+', '', text)
    text = re.sub(r'[^\w\s]', '', text)
    # print(text + "\n")
    return text

def check_cand_id(chemical_name,
                    chemical_id,
                    matches,
                    top_k):

    wrong = False
    incorrect = False
    correct_index = -1

    topk_matches = [m[1] for m in matches[:top_k]]

    if chemical_id == '-1':
        wrong = True

    # checking if token is in topK
    elif chemical_id not in topk_matches:
        correct_index = -1
        for i, match in enumerate(matches[1:]):
            if match[1] == chemical_id:
                correct_index = i+1
                break

        if correct_index == -1:
            wrong = True
        else:
            incorrect = True

    return wrong, incorrect, correct_index

def create_index(arg):
    # Define the schema with
    # LowercaseFilter + RegexTokenizer to remove space and punctuations
    # and finally NgramFilter

    index_dirs = ["./index_dirs",
                  "./index_dirs/ngram_chemical_index2014",
                  "./index_dirs/fuzzy_chemical_index2014"]

    # Create the index
    for index_dir in index_dirs:
        if not os.path.exists(index_dir):
            os.mkdir(index_dir)

    # RegexTokenizer(r"\S+\w")
    ngram_analyzers = RegexTokenizer(r"\S+") | LowercaseFilter() | NgramFilter(minsize=2,maxsize=5)
    fuzzy_analyzers = RegexTokenizer(r"\S+") | LowercaseFilter()

    ngram_schema = Schema(name=TEXT(analyzer=ngram_analyzers, stored=True),
                    id=ID(stored=True))
    fuzzy_schema = Schema(name=TEXT(analyzer=fuzzy_analyzers, stored=True),
                    id=ID(stored=True))

    print("creating index...")

    ngram_index = create_in(index_dirs[1], ngram_schema)
    fuzzy_index = create_in(index_dirs[2], fuzzy_schema)

    # Add documents to the index
    print("writing document...")
    for index in [ngram_index, fuzzy_index]:
        writer = index.writer()

        # Open the CSV file and index its contents
        csv_file = "MeSH2014.csv"
        with open(csv_file, "r", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                writer.add_document(name=row["Name"], id=row["ID"])

        # Commit the changes to the index
        writer.commit()


def mesh_search(arg, df):

    print("mesh searching begins...")

    count = 0
    rejected_match = 0
    ngram_index = open_dir("./index_dirs/ngram_chemical_index2014")
    fuzzy_index = open_dir("./index_dirs/fuzzy_chemical_index2014")

    ngram_query = QueryParser("name", schema=ngram_index.schema)
    fuzzy_query = QueryParser("name", schema=fuzzy_index.schema)

    for i, row in df.iterrows():
        if i%100==0:print(i)

        chemical_name = row['Name']
        chemical_id = row['ID']

        # normal matching
        # try:
        ngram_matches = search_chemical(
                            ngram_query,
                            ngram_index,
                            chemical_name,
                            limit=None,
        )
        fuzzy_matches = search_chemical(
                            fuzzy_query,
                            ngram_index,
                            chemical_name,
                            limit=None,
                            fuzzy=True,
                            fuzzy_distance=2,
        )

        if ngram_matches or fuzzy_matches:

            # checking ngram matches
            wrong, incorrect, correct_index = check_cand_id(chemical_name,
                                                            chemical_id,
                                                            ngram_matches,
                                                            arg.top_k)
            # checking fuzzy matches
            if (wrong or incorrect) and fuzzy_matches:
                wrong, incorrect, correct_index = check_cand_id(chemical_name,
                                                                chemical_id,
                                                                fuzzy_matches,
                                                                arg.top_k)

            # still wrong or incorrect
            matches=ngram_matches if ngram_matches else fuzzy_matches

            if wrong:
                wrong_matches.append(((chemical_name, chemical_id), matches[0]))
            elif incorrect:
                assert correct_index != -1
                incorrect_matches.append(((chemical_name, chemical_id), matches[0], correct_index))

        elif (not ngram_matches) and (not fuzzy_matches):
            if chemical_id != "-1":
                no_matches.append((chemical_name, chemical_id))

        count += 1

        # except:
        #     rejected_match+=1

    print("count, rejected_match: ", count, rejected_match)

def cal_recall(total_n):
    correct = total_n - len(wrong_matches) - len(no_matches) - len(incorrect_matches
    return (correct / (correct + incorrect_matches + wrong_matches))

def write_results(arg, df, recall):
    # write statistics to file
    file = open(arg.details_output_file, 'w')

    file.write(f"Number of incorrect matches: {len(incorrect_matches)}\n")
    file.write(f"Number of wrong matches: {len(wrong_matches)}\n")
    file.write(f"Number of no matches: {len(no_matches)}\n")
    file.write(f"Number of correct matches: {len(df) - len(wrong_matches) - len(no_matches) - len(incorrect_matches)}\n")
    file.write(f"Recall for K={args.top_k}: {recall}\n")

    df_incorrect = pd.DataFrame([], columns=['given_name', 'given_id', 'returned_name', 'returned_id', 'correct_index'])
    df_wrong = pd.DataFrame([], columns=['given_name', 'given_id', 'returned_name', 'returned_id'])
    df_no = pd.DataFrame([], columns=['chemical_name', 'chemical_id'])

    # write incorrect matches to file
    file.write('\n')
    file.write('Incorrect Matches:\n')
    for given, returned, correct_index in incorrect_matches:
        file.write(f"{given[0]}/{given[1]} --> {returned[0]}/{returned[1]} ({correct_index})\n")
        temp = pd.DataFrame([[given[0], given[1], returned[0], returned[1], correct_index]], columns=['given_name', 'given_id', 'returned_name', 'returned_id', 'correct_index'])
        df_incorrect = pd.concat([df_incorrect, temp], ignore_index=True)
    file.write('\n')

    # write wrong matches to file
    file.write('\n')
    file.write('Wrong Matches:\n')
    for given, returned in wrong_matches:
        file.write(f"{given[0]}/{given[1]} --> {returned[0]}/{returned[1]}\n")
        temp = pd.DataFrame([[given[0], given[1], returned[0], returned[1]]], columns=['given_name', 'given_id', 'returned_name', 'returned_id'])
        df_wrong = pd.concat([df_wrong, temp], ignore_index=True)
    file.write('\n')

    # write no matches to file
    file.write('\n')
    file.write('No Matches:\n')
    for chemical_name, chemical_id in no_matches:
        file.write(f"{chemical_name}/{chemical_id}\n")
        temp = pd.DataFrame([[chemical_name, chemical_id]], columns=['chemical_name', 'chemical_id'])
        df_no = pd.concat([df_no, temp], ignore_index=True)
    file.write('\n')

    file.close()

    # write dataframes to excel file
    writer = pd.ExcelWriter(arg.excel_output_file, engine='xlsxwriter')

    df_incorrect.to_excel(writer, sheet_name='incorrect_matches')
    df_wrong.to_excel(writer, sheet_name='wrong_matches')
    df_no.to_excel(writer, sheet_name='no_matches')

    writer.close()

def main():

    arg = args()

    # creating index
    if arg.create_index:
        create_index(arg)

    # searching chemical candidates in mesh
    if arg.mesh_search:
        df = pd.read_csv(arg.annotation_file)

        # filter df to remove repeated chemicals
        df = df.drop_duplicates(subset=['Name', 'ID'])

        mesh_search(arg, df)

        recall = cal_recall(len(df))

        # writing results
        write_results(arg, df, recall)


if __name__ == "__main__":
    main()
