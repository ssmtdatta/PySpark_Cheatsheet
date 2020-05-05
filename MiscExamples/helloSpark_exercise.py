# hello_spark_exercise

## Imports
from pyspark import SparkConf, SparkContext

## Module constants and global variables
APP_NAME = 'helloSpark_exercise'

# Step 4 - Perform analysis on a data file
# We have a sample file with instructors and scores. This exercise we want you to add all scores and report on results.
# Load File Instructor-Scores,
# Map name and scores into RDD,
# Add the 4 scores per instructor,
# Print the total score for each instructor
# Print average score for each instructor
# Who was top performer?
# Data File Format: Name,Score1,Score2,Score3,Score4
# Example Line: "Carlo,5,3,3,4"
# Data File Location : https://raw.githubusercontent.com/bradenrc/Spark_POT/master/Modules/SparkIntro/Instructor-Scores.txt

def main(sc):
  path = '/Users/susmitadatta/Tutorials/Python/Data/'
  scr_file = 'instructor_scores.dat'
  
  scr_rdd = sc.textFile(path+scr_file)
  
  scr_rdd = scr_rdd.map(lambda x: x.split(','))
  #print(scr_rdd.collect())

 
  tot_scr_rdd = scr_rdd.map(lambda s: [s[0], int(s[1])+int(s[2])+int(s[3])+int(s[4]) ] )
  #print(tot_scr_rdd.collect())

  avg_scr_rdd = tot_scr_rdd.map(lambda s: (s[0], s[1]/4 ) )
  #print(avg_scr_rdd.collect())

  tot_best = max(tot_scr_rdd.map(lambda s: s[1]).collect())
  #print(tot_best)
  
  best_instructor = tot_scr_rdd.filter(lambda x: x[1]==tot_best)
  if best_instructor.count() == 1:
    print("The best instructor is", best_instructor.collect()[0][0], "with a score of", best_instructor.collect()[0][1], ".")
  if best_instructor.count() > 1:
  	print("The best instructors with the score of", best_instructor.collect()[0][1], "are" )
  	for i in range(0, best_instructor.count()):
  		print(best_instructor.collect()[0][i])

   
  




## Spark driver
if __name__ == "__main__":
    # Configure Spark
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("local[*]")
    sc   = SparkContext(conf=conf)

    main(sc)