# Hello Spark
# This exercise will show you how to work with Apache Spark using Python.

# --> Step 1: Create spark context
# --> Step 2: Working with Resilient Distributed Datasets
#             - Create RDD with numbers 1 to 10
#             - Extract first line,
#             - Extract first 5 lines,
#             - Create RDD with string "Hello Spark",
#             - Extract first line.
# --> Step 3: - Count number of lines from README.md file: 
#             https://github.com/bradenrc/Spark_POT/blob/master/Modules/SparkIntro/README.md
#             - Separate the lines the contains the word "spark"
#             - How many lines in the file has the word spark?
#

## Imports
from pyspark import SparkConf, SparkContext
import requests

## Module constants and global variables
APP_NAME = 'hello_spark'


# Convert the file to an RDD,
# Count the number of lines with the word "Spark" in it. 
# Type:
# !wget https://github.com/bradenrc/Spark_POT/blob/master/Modules/SparkIntro/README.md

def createRDD(num1, num2):
	n = range(num1, num2+1)
	return sc.parallelize(n)

def helloSpark(string):
	return sc.parallelize([string])

def main(sc):

  # step 2:
  # NB: note the difference between first() and take() method.
  range_rdd = createRDD(1, 10)
  print('The first of the numbers ranging from 1 to 10: \n', range_rdd.first())
  print('The first of the numbers ranging from 1 to 10: \n', range_rdd.take(1))
  print('The first 5 of the numbers ranging from 1 to 10: \n', range_rdd.take(5))
  
  # NB: the input string in read as a list so the the first element of the list if the first line
  string_rdd = helloSpark('Hello Spark')
  print(string_rdd.first())

  # Step 3: TBD
  #readme = requests.get('https://github.com/bradenrc/Spark_POT/blob/master/Modules/SparkIntro/README.md')
  #print(type(readme))

  # A variation of Step 3 
  # path = '/Users/susmitadatta/Tutorials/Python/Data/'
  # textfile = 'text.txt'
  # text_rdd = sc.textFile(path+textfile)
  
  # lines_rdd = text_rdd.map(lambda line: line.split(". "))
  # t = lines_rdd.filter(lambda lines: 'Characters' in lines[0])
  # print(t.collect())



# Step 1: Spark Driver
if __name__ == "__main__":
    # Configure Spark
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("local[*]")
    sc   = SparkContext(conf=conf)

    main(sc)