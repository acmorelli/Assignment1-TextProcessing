"""workflow
1. one JSON per line
2. tokenize the review text
3. lowercase the text 
4. remove stop words and one-letter
5. emit the ky, cat pair 
"""
from mrjob.job import MRJob
import json
import re

class PreprocessReviews(MRJob):
    
    JOBCONF = {
        'mapreduce.map.output.compress': 'true',
        'mapreduce.map.output.compress.codec': 'org.apache.hadoop.io.compress.GzipCodec',
        'mapreduce.job.reduces': '5'
    }

    def configure_args(self):
        super(PreprocessReviews, self).configure_args()
        self.add_file_arg('--stopwords', help='Path to stopwords.txt')

    def mapper_init(self):
        self.stopwords = set()
        with open(self.options.stopwords, 'r') as f:
            for line in f:
                self.stopwords.add(line.strip().lower())

    def mapper(self, _, line):
        try:
            review = json.loads(line)
            category = review.get('category', None)
            review_text = review.get('reviewText', '')

            if category and review_text:
                tokens = re.split(r"[\s\d\(\)\[\]\{\}\.\!\?,;:\+=\-_\"\'\`~#@&\*\%€\$§\\/]+", review_text)
                for token in tokens:
                    token = token.lower().strip()
                    if token and token not in self.stopwords and len(token) > 1:
                        yield (token, category), 1
        except Exception as e:
            # Ignore badly formatted lines
            pass

    def combiner(self, key, counts):
        """sums local counts on each mapper"""
        yield key, sum(counts)

    def reducer(self, key, counts):
        """sums partially combined counts from all mappers"""
        yield key, sum(counts)

if __name__ == '__main__':
    PreprocessReviews.run()


