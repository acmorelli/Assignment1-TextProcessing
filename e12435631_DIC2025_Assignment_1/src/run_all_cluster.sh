#!/bin/bash

# inside run_all_cluster.sh
USER_ID=e12435631

# clear old outputs 
hdfs dfs -rm -r /user/$USER_ID/assignment1/output_preprocess/
hdfs dfs -rm -r /user/$USER_ID/assignment1/output_doccounts/
hdfs dfs -rm -r /user/$USER_ID/assignment1/output_chisquare/
hdfs dfs -rm -r /user/$USER_ID/assignment1/output_final/

# 1. Tokenization and cleaning
python3 src/preprocess.py -r hadoop \
    --python-bin python3 \
    --stopwords stopwords.txt \
    -o /user/$USER_ID/assignment1/output_preprocess/ \
    hdfs:///user/dic25_shared/amazon-reviews/full/reviewscombined.json

# 2. Count docs per category
python3 src/count_documents_per_category.py -r hadoop \
    --python-bin python3 \
    -o /user/$USER_ID/assignment1/output_doccounts/ \
    hdfs:///user/dic25_shared/amazon-reviews/full/reviewscombined.json

# 3. Chi-Square Calculation
python3 src/chiSquare_calculator.py -r hadoop \
    --python-bin python3 \
    --doc_counts /user/$USER_ID/assignment1/output_doccounts/part-* \
    -o /user/$USER_ID/assignment1/output_chisquare/ \
    /user/$USER_ID/assignment1/output_preprocess/part-*

# 4. Select Top-75 Discriminators and Merge Dict
python3 src/selector_discriminators.py -r hadoop \
    --python-bin python3 \
    -o /user/$USER_ID/assignment1/output_final/ \
    /user/$USER_ID/assignment1/output_chisquare/part-*
