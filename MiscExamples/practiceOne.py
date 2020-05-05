## Spark Application - execute with spark-submit
from pyspark import SparkConf, SparkContext

#from pyspark.sql import SQLContext
#from pyspark.sql.types import *

APP_NAME = 'Spark App'

INPUT_PATH = "/Users/susmitadatta/Tutorials/Python/Data/"
INPUT_FILE = "sample_data.csv"

def stringManupilation(line):
	for i in range(0, len(line)):
		line[i] = line[i].replace('"','')
	return line




def main(sc):
  file_contents = sc.textFile(INPUT_PATH+INPUT_FILE)
  file_contents.persist()
  num_lines = file_contents.count()
  print("number of lines in csv file", num_lines)


  # view the first n rows
  first5 = file_contents.take(5)
  print(len(first5), type(first5))

  # view n random lines with takeSample method. 
  # takeSample takes three arguments: takeSample("if replacement", "number of samples",  "seed"). 
  # set "if replacement" = 1  (meaning True)
  # set "number of samples" = n (where is an intiger such as 5, 10, etc.)
  # seed is optional set to an intiger value.
  rand10 = file_contents.takeSample(1, 10, 111)
  print(len(rand10), type(rand10))

  # separate each line of the csv (rdd) into a list element
  content = file_contents.map(lambda x: x.split(','))
  #line = content.take(3)[1]
  #print(line, len(line))
  content = content.map(stringManupilation)
  # line = content.take(3)[1]
  # print(line, len(line))

  header = content.take(1)
  print(header)

  data = content.take()





if __name__ == "__main__":
    # Configure Spark
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("local[*]")
    sc   = SparkContext(conf=conf)

    main(sc)