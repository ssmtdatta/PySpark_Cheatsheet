from pyspark import SparkConf, SparkContext

from pyspark.sql import SQLContext
from pyspark.sql.types import *

from datetime import *
from dateutil.parser import parse


import os
os.system('sox input.wav -b 24 output.aiff rate -v -L -b 90 48k')


conf = SparkConf().setMaster("local[*]")
sc   = SparkContext(conf=conf)
sqlc = SQLContext(sc)


PATH = "/Users/susmitadatta/Tutorials/Python/Data/"
FILE = "nyctaxisub.csv"


taxi_data = sc.textFile(PATH+FILE)


taxi_header = taxi_data.first()   # first() outputs a string  
                                  # take() outputs a list | header = taxi_data.take(1) 


header_string = taxi_header.replace('"', '')


# sql way
fields = [StructField(field_name, StringType(), True) for field_name in header_string.split(',')]

# edit field name
fields[0].name = 'id'
fields[1].name = 'rev'
# edit field data type
fields[2].dataType = TimestampType()
fields[3].dataType = FloatType()
fields[4].dataType = FloatType()
fields[7].dataType = IntegerType()
fields[8].dataType = TimestampType()
fields[9].dataType = FloatType()
fields[10].dataType = FloatType()
fields[11].dataType = IntegerType()
fields[13].dataType = FloatType()
fields[14].dataType = IntegerType()

def format(p):
  p[0] = p[0], 
  #p[1], parse(p[2].strip('"')), float(p[3]), float(p[4]) , p[5], p[6] , int(p[7]), parse(p[8].strip('"')), float(p[9]), float(p[10]), int(p[11]), p[12], float(p[13]), int(p[14]), p[15] ))
 

# rdd format
header = sc.parallelize(taxi_data.take(1))    # take() outputs a list                          
data = taxi_data.subtract(header)             # subtract() works for lists ?

data_rows = data.map(lambda s: s.split(',')).map(lambda p: (p[0], p[1], parse(p[2].strip('"')), float(p[3]), float(p[4]) , p[5], p[6] , int(p[7]), parse(p[8].strip('"')), float(p[9]), float(p[10]), int(p[11]), p[12], float(p[13]), int(p[14]), p[15] ))

schema = StructType(fields)
df = sqlc.createDataFrame(data_rows, schema)


print(df.head(3))

           



