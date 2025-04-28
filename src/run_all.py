import os 

import os

def run_preprocessing(input_file, stopwords_file, output_file):
    print("Running preprocessing...")
    os.system(f"python src/preprocess.py {input_file} --stopwords {stopwords_file} > {output_file}")

def run_doc_counting(input_file, output_file):
    print("Running document counting...")
    os.system(f"python src/count_documents_per_category.py {input_file} > {output_file}")

def run_chi_square(word_category_counts_file, doc_counts_file, output_file):
    print("Running chi-square calculation...")
    os.system(f"python src/chiSquare_calculator.py {word_category_counts_file} --doc_counts {doc_counts_file} > {output_file}")

def run_top_terms(chi_square_output_file, output_file):
    print("Running top terms selection...")
    os.system(f"python src/selector_discriminators.py {chi_square_output_file} > {output_file}")

if __name__ == "__main__":
    # Input and output paths
    reviews_file = "reviews_devset.json"      # Your local dev set
    stopwords_file = "stopwords.txt"           # Your stopwords
    word_category_counts = "word_category_counts.txt"
    doc_counts = "doc_counts.txt"
    chi_square_output = "chi_square_output.txt"
    final_output = "output.txt"

    # Run steps
    run_preprocessing(reviews_file, stopwords_file, word_category_counts)
    run_doc_counting(reviews_file, doc_counts)
    run_chi_square(word_category_counts, doc_counts, chi_square_output)
    run_top_terms(chi_square_output, final_output)

    print("✅ Pipeline completed successfully. Check output.txt")
