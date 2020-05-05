#word count example

'''
[1] Create a base RDD from a list of words.
[2] Pluralize the words. Define a function makePlural that adds to the singular word to make them plural.
[3]
'''

## Imports
from pyspark import SparkConf, SparkContext

sc   = SparkContext()




def makePlural(word):
  '''
    Method: 
	   Adds an 's' to `word`.
    Note:
        This is a simple function that only adds an 's'.  No attempt is made to follow proper
        pluralization rules.
    Args:
        word (str): A string.
    Returns:
        str: A string with 's' added to it.
  '''

  if isinstance(word, str):
    print('The input word:', word, 'is a string.')
    if word.isalpha():
      print('The input word:', word, 'contains letters from the alphabet only.')
      return word + 's'

def wordLength(word):
  if isinstance(word, str):
    print('The input word:', word, 'is a string.')
    if word.isalpha():
      print('The input word:', word, 'contains letters from the alphabet only.')
      return len(word)


wordsList = ['cat', 'elephant', 'rat', 'rat', 'cat']
wordsRDD = sc.parallelize(wordsList, 4)


plural = wordsRDD.map(makePlural)
plural = wordsRDD.map(lambda x: x + 's')



#Length of each word
#Now use map() and a lambda function to return the number of characters in each word. 
#We'll collect this result directly into a variable.
plural_len = (plural.map(wordLength))
print(plural_len.collect())

# create pair RDD, with word and 1
word_pairs = wordsRDD.map(lambda x: (x, 1))
print(word_pairs.collect())


word_dict = {'cat': 1, 'cat': 1, 'bat': 1, 'rat': 1, 'cat': 1, 'cat': 1, 'bat': 1, 'rat': 1}
word_dict_rdd = sc.parallelize(word_dict)
#counting with pair RDDs
'''
method 1:
  groupByKey()
'''
# wc1 = word_pairs.groupByKey().map(lambda x: (x[0], sum(x[1])))
# print(wc1.collect())

# wc2 = word_pairs.groupByKey().mapValues(lambda x: sum(x))
# print(wc2.collect())

'''
method 2:
  reduceByKey()
  # reduce and reduceByKey work the same. reduceByKey performs an extra step of separating the keys 
  # and reduce the keys separately.
'''
wc3 = word_pairs.reduceByKey(lambda a, b: a + b)  
print(wc3.collect())

# calculate the number of unique words
unique_words = word_pairs.map(lambda x: x[0]).distinct()
print(unique_words.count())









