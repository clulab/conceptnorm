import pandas as pd
import numpy as np
from whoosh.index import open_dir
from whoosh.qparser import QueryParser, FuzzyTermPlugin
import argparse


no_matches = [] # no returned matches
wrong_matches = [] # returned matches, but none of them are correct
incorrect_matches = [] # returned matches, but the first one is not correct

def args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--index_dir", help="path to index directory", default="chemicals_index2014")
    parser.add_argument("-ngi", "--n_gram_index_dir", help="path to index directory", default="n_gram_chemicals_index2014")
    parser.add_argument("-i_nospace", "--no_space_index_dir", help="path to index directory", default="no_space/chemicals_index2014")
    parser.add_argument("-ngi_nospace", "--no_space_n_gram_index_dir", help="path to index directory", default="no_space/n_gram_chemicals_index2014")
    parser.add_argument("-a", "--annotation_file", help="path to annotation file", default="chemical_annotations.csv")
    parser.add_argument("-d", "--details_output_file", help="path to details.txt output file", type=str, default="results/chemical_matches_details2014.txt")
    parser.add_argument("-x", "--excel_output_file", help="path to excel output file", type=str, default="results/chemical_matches2014.xlsx")
    parser.add_argument("-ns", "--no_space", help="if want to include no_space too", type=bool, default=False)

    args = parser.parse_args()
    return args


def search_chemical(index, query_string, limit=None, fuzzy=False, fuzzy_distance=1):

    query = QueryParser("name", schema=index.schema)

    # For fuzzy search
    if fuzzy:
        query.add_plugin(FuzzyTermPlugin())
        query_string = query_string+f"~{fuzzy_distance}"

    with index.searcher() as searcher:
        query = query.parse(query_string)
        results = searcher.search(query, limit=limit)

        matches = []
        for result in results:
            matches.append((result["name"], result["id"]))

        return matches

def check_match(matches, chemical_id):
    correct_index = -1
    for i, match in enumerate(matches[1:]):
        if match[1] == chemical_id:
            correct_index = i+1
            break
    return correct_index

def check_cand_id(chemical_name, chemical_id, matches):
    wrong = False
    incorrect = False
    correct_index = -1

    top20_matches = [m[1] for m in matches[:20]]

    if chemical_id == '-1':
        wrong = True

    # checking if token is in top20
    elif chemical_id not in top20_matches:  # elif chemical_id != matches[0][1]:
        correct_index = check_match(matches, chemical_id)

        if correct_index == -1:
            wrong = True
        else:
            incorrect = True

    return wrong, incorrect, correct_index

def calculate_recall():
    pass

def main():

    arg = args()
    index = open_dir(arg.index_dir)
    n_gram_index_dir = open_dir(arg.n_gram_index_dir)
    no_space_index_dir = open_dir(arg.no_space_index_dir)
    no_space_n_gram_index_dir = open_dir(arg.no_space_n_gram_index_dir)

    df = pd.read_csv(arg.annotation_file)

    # filter df to remove repeated chemicals
    df = df.drop_duplicates(subset=['Name', 'ID'])

    count = 0
    for i, row in df.iterrows():
        if i%100==0:print(i)

        chemical_name = row['Name']
        chemical_id = row['ID']

        # normal matching
        matches = search_chemical(
                            index,
                            chemical_name,
        )

        # fuzzy matching
        fuzzy_matches = search_chemical(
                            index,
                            chemical_name,
                            fuzzy=True,
                            fuzzy_distance=2,
        )

        if arg.no_space:
            no_space_fuzzy_matches = search_chemical(
                                no_space_index_dir,
                                chemical_name,
                                fuzzy=True,
                                fuzzy_distance=2,
            )

        # candidate N-Gram matching
        n_gram_matches = search_chemical(
                            n_gram_index_dir,
                            chemical_name,
                            limit=20,
        )

        if arg.no_space:
            no_space_n_gram_matches = search_chemical(
                                no_space_n_gram_index_dir,
                                chemical_name,
                                limit=20,
            )

        if matches:
            """
            For all matches, we will concentrate on the top 20.
            If the match is in top 20, we will consider it as
            success.
            """
            # normal match
            wrong, incorrect, correct_index = check_cand_id(chemical_name, chemical_id, matches)

            # if (incorrect or wrong) and fuzzy match exists
            if incorrect or wrong:
                if fuzzy_matches:
                    wrong, incorrect, correct_index = check_cand_id(chemical_name, chemical_id, fuzzy_matches)

            # if still incorrect and wrong are there, the go for candidate n-gram
            if incorrect or wrong:
                if n_gram_matches:
                    wrong, incorrect, correct_index = check_cand_id(chemical_name, chemical_id, n_gram_matches)

            if arg.no_space:
                # if still incorrect and wrong are there, the go for fuzzy match but with no space
                if incorrect or wrong:
                    if no_space_fuzzy_matches:
                        wrong, incorrect, correct_index = check_cand_id(chemical_name, chemical_id, no_space_fuzzy_matches)

                # if still incorrect and wrong are there, the go for candidate n-gram with no space
                if incorrect or wrong:
                    if no_space_n_gram_matches:
                        wrong, incorrect, correct_index = check_cand_id(chemical_name, chemical_id, no_space_n_gram_matches)

            if wrong:
                wrong_matches.append(((chemical_name, chemical_id), matches[0]))
            elif incorrect:
                assert correct_index != -1
                incorrect_matches.append(((chemical_name, chemical_id), matches[0], correct_index))

        elif not matches and not fuzzy_matches and not n_gram_matches:
            if chemical_id != '-1':
                no_matches.append((chemical_name, chemical_id))

        count += 1

    # write statistics to file
    file = open(arg.details_output_file, 'w')

    file.write(f"Number of incorrect matches: {len(incorrect_matches)}\n")
    file.write(f"Number of wrong matches: {len(wrong_matches)}\n")
    file.write(f"Number of no matches: {len(no_matches)}\n")
    file.write(f"Number of correct matches: {len(df) - len(wrong_matches) - len(no_matches) - len(incorrect_matches)}\n")

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


if __name__ == '__main__':
    main()
