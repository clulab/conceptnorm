import pandas as pd
from rapidfuzz import fuzz


def check_fragment(word, names, ids):
    possible_names = list()
    found = False
    for i in range(1, len([char for char in word])):
        token = word[:-i]
        # check the fragment
        for item in names:
            if token in item or token.capitalize() in item:
                possible_names.append(item)
                found = True
        if found:
            return token, possible_names


def best_top_K_match(name, token, possible_match):
    assert (token in name) or (token.capitalize() in name)

    ratios = list()
    for _pm in possible_match:
        ratios.append(fuzz.ratio(name, _pm))

    top1, top2 = sorted(ratios, reverse=True)[:2]
    return {
        possible_match[ratios.index(top1)]: top1,
        possible_match[ratios.index(top2)]: top2,
    }


if __name__ == "__main__":
    # open Mesh.csv
    mesh_data = pd.read_csv("MeSH.csv")
    no_match_tokens = pd.read_excel(
        "chemical_matches2014.xlsx", sheet_name="no_matches"
    )

    mesh_names = list(mesh_data["Name"])
    mesh_ids = list(mesh_data["ID"])

    no_match_names = list(no_match_tokens["chemical_name"])

    # get all possible chemical names that either
    # have the word or a fragment of it.
    tokens = list()
    names_containing_fragments = list()
    idxs = list()
    ratio_file = open("no_match_fuzz_ratio_file.txt", "w")

    for idx, word in enumerate(no_match_names):
        try:
            token, possible_names = check_fragment(word, mesh_names, mesh_ids)
            tokens.append(token)
            names_containing_fragments.append(possible_names)
            idxs.append(idx)
        except:
            # ratio_file.write(f"{word} has some problem!! \n")
            print(f"{word} has some problem!!")

    for i in idxs:
        try:
            if len(names_containing_fragments[i]) > 0:
                if len(names_containing_fragments[i]) >= 2:
                    top_K_match = best_top_K_match(
                        no_match_names[i],
                        tokens[i],
                        names_containing_fragments[i],
                    )
                    ratio_file.write(
                        f"{no_match_names[i]} \t {tokens[i]} \t {top_K_match} \n"
                    )
                else:
                    ratio = fuzz.ratio(
                        no_match_names[i], names_containing_fragments[i]
                    )
                    top_K_match = {names_containing_fragments[i][0]: ratio}

                    ratio_file.write(
                        f"{no_match_names[i]} \t {tokens[i]} \t {top_K_match} \n"
                    )
        except:
            # ratio_file.write(f"{no_match_names[i]} has some problem!! \n")
            print(f"{no_match_names[i]} has some problem!!")
