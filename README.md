# conceptnorm

## Essential Files:
### MeSH2014.csv
Mesh2014.csv is a file created by parsing and combining desc2014.xml and supp2014.xml downloaded from the official MeSH website. 
### chemicals_annotated.csv
chemicals_annotated.csv is a file created by parsing CDR_DevelopmentSet.BioC.xml from the BC5CDR corpus.

## Using the Index
Unzip the chemicals_index2014.zip file to access the index.


### create_index.py
Creates the index from the given MeSH csv file. First argument is the path to MeSH csv data. Second parameter is the output name of the index directory.
```
python3 create_index.py MeSH2014.csv chemicals_index2014
```

### candidate_generator.py
Returns the list of matches given the search term.
```
python3 candidate_generator.py -i chemicals_index2014 chemical_name
```

### mesh_search.py
Uses the index to search for all the terms in the annotated data, creates two files: a text file and an excel file detailing all the correct and incorrect matches. Default parameters are defined in the code. 
```
python3 mesh_search.py
```

### Order of processing:
1. Create your index using create_index.py, this will output the index directory.
2. Use candidate_generator.py with desired index directory to search for a given string (ideally a chemical name).
3. Use mesh_search.py to see how your index and search method performs with the annotated data from BC5CDR (chemical_annotations.csv).

