from mrjob.job import MRJob
from mrjob.step import MRStep
import heapq
import json

class SelectorDiscriminators(MRJob):

    def configure_args(self):
        super(SelectorDiscriminators, self).configure_args()
        self.top_k = 75

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_select,
                reducer_init=self.reducer_init_topk,
                reducer=self.reducer_topk
            ),
            MRStep(
                mapper=self.mapper_collect_terms,
                reducer_init=self.reducer_init_merge,
                reducer=self.reducer_merge_terms,
                reducer_final=self.reducer_final_merge
            )
        ]

    # First MRStep
    def mapper_select(self, _, line):
        key, value = line.strip().split('\t')
        category, word = json.loads(key)
        chi_square = float(value)
        yield category, (chi_square, word)

    def reducer_init_topk(self):
        self.all_words = set()

    def reducer_topk(self, category, chi_word_pairs):
        top_terms = heapq.nlargest(self.top_k, chi_word_pairs)

        for chi, word in top_terms:
            self.all_words.add(word)

        formatted = " ".join(f"{word}:{chi:.4f}" for chi, word in top_terms)
        yield category, formatted

        # 🔥 Yield the words individually for second step
        for word in self.all_words:
            yield "__WORDS__", word

    # Second MRStep
    def mapper_collect_terms(self, key, value):
        if key == "__WORDS__":
            yield "__MERGED__", value
        else:
            yield key, value

    def reducer_init_merge(self):
        self.all_collected_words = set()

    def reducer_merge_terms(self, key, values):
        if key == "__MERGED__":
            for word in values:
                self.all_collected_words.add(word)
        else:
            # Just pass categories + formatted string forward
            yield key, list(values)[0]

    def reducer_final_merge(self):
        if self.all_collected_words:
            sorted_words = sorted(self.all_collected_words)
            yield None, " ".join(sorted_words)

if __name__ == '__main__':
    SelectorDiscriminators.run()
