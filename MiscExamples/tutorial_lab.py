#
# Introduction to Big Data with Apache Spark (CS100-1x)
# Module 2: Spark Tutorial Lab
#
# === Susmita Datta === #


## Imports
from pyspark import SparkConf, SparkContext


## Module constants and global variables
APP_NAME = 'tutorial'
# global XMIN, XMAX
# XMIN = 1 
# XMAX = 10000



# Create a Python collection of integers in the range of 1 .. 10000


def rdd_of_int_range(xmin, xmax):
  data = range(xmin, xmax+1)
  data_rdd = sc.parallelize(data)
  #data_rdd.setName('My first RDD')
  #print('the data type of range RDD is:{}'.format(type(data_rdd)))
  #print('the data type of range RDD is: {}'.format(data_rdd.id()))
  #print(data_rdd.toDebugString())
  #help(data_rdd)
  return data_rdd


def sub_one(x):
	return x-1

def filter_lt_10(x):
  if x < 10:
    return x
  else:
  	return False

def filter_even(x):
  if x%2 == 0:
  	return x
  else:
  	return False




def main(sc):  
  # create an rdd of int range 1 to 10,000
  xmin, xmax = 1, 10000	
  data_rdd = rdd_of_int_range(xmin, xmax)

  #Subtract one from each value of the rdd
  # sleek way
  sub_one_rdd = data_rdd.map(lambda x: x-1)
  #print(sub_one_rdd.take(10))
  # non-sleek way
  #sub_one_rdd = data_rdd.map(sub_one)
  #print(sub_one_rdd.take(10))

  #Filter values less than 10
  # sleek way
  lt10_rdd = data_rdd.filter(lambda x: x < 10)
  #print(lt10_rdd.collect())
  # non-sleek way
  #lt10_rdd = data_rdd.filter(filter_lt_10)
  #print(lt10_rdd.collect())

  #Filter even values
  # sleek way
  even_rdd = data_rdd.filter(lambda x: x%2==0)
  #print(even_rdd.take(10))
  # non-sleek way
  #even_rdd = data_rdd.filter(filter_even)
  #print(even_rdd.take(10))


  #takeOrdered() returns the list sorted in ascending order. 
  #top() action is similar to takeOrdered() except that it returns the list in descending order.
  #takeOrdered(4, lambda s: -s)
  rdd = sc.parallelize([10, 100, 1000, 10])
  print(rdd.reduce(lambda a, b: a + b))
  #print(rdd.reduce(lambda a, b: a - b))
  #print(rdd.reduce(lambda a, b: b - a))

  # takeSample reusing elements
  lt10_rdd.takeSample(withReplacement=True, num=6)
  # takeSample without reuse
  lt10_rdd.takeSample(withReplacement=False, num=6)
  # Set seed for predictability
  lt10_rdd.takeSample(withReplacement=False, num=6, seed=500)

  repetitive_rdd = sc.parallelize(['A', 'B', 'C', 'X', 'M', 'B', 'C', 'X','A', 'B', 'C', 'A', 'B', 'C', 'X', 'M', 'B', 'C', 'X','A', 'B' ])
  count_by_value = repetitive_rdd.countByValue()
  #print(type(count_by_value), count_by_value)

  # map vs. flatMap
  # Use map
  words = ['cat', 'elephant', 'rat', 'rat', 'cat']
  words_rdd = sc.parallelize(words, 4)
  
  after_map = words_rdd.map(lambda x: (x, x + 's'))
  after_flatMap = words_rdd.flatMap(lambda x: (x, x + 's'))
  after_flatMap = after_flatMap.flatMap(lambda x: x)
  # View the results 
  print(after_map.collect())
  print(after_flatMap.collect())
  # View the number of elements in the RDD
  print(after_map.count())
  print(after_flatMap.count())

  after_map = words_rdd.map(lambda x: x + 's')
  after_flatMap = words_rdd.flatMap(lambda x: x + 's')
  # View the results 
  print(after_map.collect())
  print(after_flatMap.collect())
  # View the number of elements in the RDD
  print(after_map.count())
  print(after_flatMap.count())






  








if __name__ == "__main__":
    # Configure Spark
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("local[*]")
    sc   = SparkContext(conf=conf)

    main(sc)

