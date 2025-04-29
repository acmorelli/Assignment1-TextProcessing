# python src/count_documents_per_category.py reviews_devset.json > doc_counts.txt



from mrjob.job import MRJob
import json

class CountDocumentsPerCategory(MRJob):
    
    JOBCONF = {
        'mapreduce.map.output.compress': 'true',
        'mapreduce.map.output.compress.codec': 'org.apache.hadoop.io.compress.GzipCodec',
        'mapreduce.job.reduces': '5'
    }

    def mapper(self, _, line):
        try:
            review = json.loads(line)
            category = review.get('category', None)
            if category:
                yield (category, 'CATEGORY'), 1
                yield ('TOTAL', 'TOTAL'), 1
        except Exception:
            pass

    def combiner(self, key, counts):
        yield key, sum(counts)

    def reducer(self, key, counts):
        yield key, sum(counts)

if __name__ == '__main__':
    CountDocumentsPerCategory.run()
