"""
workflow: 
1. load document counts (category and total documents)
2. process kv, cat pair counts
3. calcualte chi square
4. emit kv, cat -> chi square value
"""

# python src/chiSquare_calculator.py word_category_counts.txt --doc_counts doc_counts.txt > chi_square_output.txt

from mrjob.job import MRJob
from mrjob.step import MRStep
import json

class ChiSquareCalculator(MRJob):
    
    JOBCONF = {
        'mapreduce.map.output.compress': 'true',
        'mapreduce.map.output.compress.codec': 'org.apache.hadoop.io.compress.GzipCodec',
        'mapreduce.job.reduces': '5'
    }

    def configure_args(self):
        super(ChiSquareCalculator, self).configure_args()
        self.add_file_arg('--doc_counts', help='Path to document counts output')

    def mapper_init(self):
        self.category_doc_counts = {}
        self.total_docs = 0

        with open(self.options.doc_counts, 'r') as f:
            for line in f:
                key, count = line.strip().split('\t')
                category, label = json.loads(key)
                count = int(count)

                if label == "CATEGORY":
                    self.category_doc_counts[category] = count
                elif label == "TOTAL":
                    self.total_docs = count

    def mapper(self, _, line):
        # Input line format: ["word", "category"] \t count
        key, count = line.strip().split('\t')
        word, category = json.loads(key)
        count = int(count)

        # Emit key = word, value = (category, count)
        yield word, (category, count)

    def reducer_group_by_word(self, word, values):
        """
        Group all counts for each word:
        - ("category", count) from each category
        """
        counts = {}
        total_word_count = 0

        for category, count in values:
            counts[category] = counts.get(category, 0) + count
            total_word_count += count

        # Emit word and all needed data
        yield word, (counts, total_word_count)

    def reducer_calculate_chi_square(self, word, values):
        """
        Final step: calculate chi-square per (word, category)
        """

        # 🔥 Load document counts again (because we are in a new step)
        if not hasattr(self, 'category_doc_counts'):
            self.category_doc_counts = {}
            self.total_docs = 0
            with open(self.options.doc_counts, 'r') as f:
                for line in f:
                    key, count = line.strip().split('\t')
                    category, label = json.loads(key)
                    count = int(count)

                    if label == "CATEGORY":
                        self.category_doc_counts[category] = count
                    elif label == "TOTAL":
                        self.total_docs = count

        for counts, total_word_count in values:
            for category, A in counts.items():
                B = total_word_count - A
                category_total = self.category_doc_counts.get(category, 0)

                C = category_total - A
                D = (self.total_docs - category_total) - B

                numerator = (A * D - B * C) ** 2
                denominator = (A + B) * (C + D) * (A + C) * (B + D)

                if denominator != 0:
                    chi_square = (self.total_docs * numerator) / denominator
                    yield (category, word), chi_square


    def steps(self):
        return [
            MRStep(mapper_init=self.mapper_init,
                   mapper=self.mapper,
                   reducer=self.reducer_group_by_word),
            MRStep(reducer=self.reducer_calculate_chi_square)
        ]

if __name__ == '__main__':
    ChiSquareCalculator.run()
