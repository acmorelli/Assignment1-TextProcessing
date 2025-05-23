# Assignment 1 - Text Processing Fundamentals
Goal: 75 most discriminative terms per category, based on chi square test

Pre processing general workflow:
1. Break the text into tokens (terms), based on whitespaces and symbols (indicated in the assignment pdf)
2. Lowercase all text for fair analysis
3. Remove common words from stopwords.txt and one-character terms.

Using dev dataset for local development:
1. Loop through JSON files and their lines
2. Pre process workflow
3. Emit for each <keyvalue, category> -> 1
4. Combine - Locally sum counts (reduces network load)

5. Reducer - aggregate counts <keycalue, category> -> total_count

6. MapReduce job for Chi Squared test - calculate for each <kv, cat>  pair:
a. times the term appears in the category
b. times it appears in other categories
c. number of documents in the category without the term
d. number of documents in other categories without the term

Compute Chi Square

7. Select top 75 most discrimating per category
7.1 group by category
7.2 desceding chi squared 
7.3 filter first 75

8. output for each category: 
<category> term: chi score

9. merge terms
Collect all terms used in any category
Sort alphabetically
output space separated in a single line after all categories

10. generate .txt output file
