#################################################################################################################################################################
# The script takes in two command-line arguments: a directory containing the pipeline stats, and an option to select either the slowest or fastest pipeline runs
# The code uses PySpark to read in JSON data from a folder, explode the "stats" column, pivot the data by "state", and calculate the time difference between each state. 
# It then creates a new dataframe containing the time difference between each state transition, and finally uses the "stack" function to reshape the dataframe for further analysis. 
# The final dataframe has one column for the ID, and one column for each state transition, representing the time difference between the states.
#e.g python <script.py> -d /path/to/directory -o [<slowest/fastest>
#################################################################################################################################################################

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import argparse
import os
import time

start_time = time.time()

# Argparse module to parse command line arguements & it creates argumentparser object & adds 2 argument
parser = argparse.ArgumentParser()
parser.add_argument("-d", "--directory", help="Directory containing pipeline stats")
parser.add_argument("-o", "--option", choices=["slowest", "fastest"], help="Select slowest or fastest pipeline runs")
#parse_args() method on the parser object to parse the command-line arguments and store the result in the args variable
args = parser.parse_args()

#SparkSession is the entry point to the Spark functionality and provides a single point of access to all Spark functionality
#creates a new Spark session or reuses an existing one
#sets the name of the Spark application
#method gets an existing Spark session or creates a new one if none exists
spark = SparkSession.builder.appName("Json Processing").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

folder = args.directory

if os.path.exists(folder):
    # single-line mode can be split into many parts & read in parallel but In multi-line mode file loaded as a
    # whole entity & cannot be split
    inputDF = spark.read.option("multiline", "true").json(folder)
    # function takes an array column and creates a new row for each element in the array
    statsDF = inputDF.select(explode("stats").alias("stats"))
    # Unique values of the "state" column become separate columns,with the earliest "utcTimeStamp" value for each "id" and "state" combination.
    metricDF = statsDF.groupBy("stats.id").pivot("stats.state").agg(first("stats.utcTimeStamp").alias("utcTimeStamp"))
else:
    print("File not found at the given path: ", folder)

#Each tuple in the list represents a transition between two states in a processing pipeline
pipelinesMap = [("NEW", "QUEUED_FOR_PROCESSING"),
        ("QUEUED_FOR_PROCESSING", "PRE_PROCESSING"),
        ("PRE_PROCESSING", "FILE_TO_TIFF_CONVERSION_PROCESSING"),
        ("FILE_TO_TIFF_CONVERSION_PROCESSING", "FILE_TO_TIFF_CONVERSION_FINISHED"),
        ("FILE_TO_TIFF_CONVERSION_FINISHED", "OCR_PROCESSING"),
        ("OCR_PROCESSING", "OCR_FINISHED"),
        ("OCR_FINISHED", "PRE_PROCESSING_FINISHED"),
        ("PRE_PROCESSING_FINISHED", "PIPELINE_PROCESSING"),
        ("PIPELINE_PROCESSING", "PIPELINE_FINISHED"),
        ("PIPELINE_FINISHED", "POST_PROCESSING"),
        ("POST_PROCESSING", "DOCUMENT_PROCESSED")]

#Loops through the list of tuples,for each tuple adds a new column
#The name of the new column is a concatenation of the two state names
#The value in the new column is the difference between the timestamps in the two states
pipelineDF = metricDF
for (v1, v2) in pipelinesMap:
    pipelineDF = pipelineDF.withColumn(f"{v1}____{v2}", col(v2) - col(v1))


#Select only specific columns.The selected columns are the "id" column and the new columns generated from the loop
pipelineDF = pipelineDF.select("id",
                               *[f"{v1}____{v2}" for (v1, v2) in pipelinesMap]
                              )

pipelineStatsDF = pipelineDF.select(col("ID"),
                                    expr("stack(11, " +
                                         "'A.NEW -> QUEUED_FOR_PROCESSING', NEW____QUEUED_FOR_PROCESSING, " +
                                         "'B.QUEUED_FOR_PROCESSING -> PRE_PROCESSING', QUEUED_FOR_PROCESSING____PRE_PROCESSING, " +
                                         "'C.PRE_PROCESSING -> FILE_TO_TIFF_CONVERSION_PROCESSING', PRE_PROCESSING____FILE_TO_TIFF_CONVERSION_PROCESSING, " +
                                         "'D.FILE_TO_TIFF_CONVERSION_PROCESSING -> FILE_TO_TIFF_CONVERSION_FINISHED', FILE_TO_TIFF_CONVERSION_PROCESSING____FILE_TO_TIFF_CONVERSION_FINISHED, " +
                                         "'E.FILE_TO_TIFF_CONVERSION_FINISHED -> OCR_PROCESSING', FILE_TO_TIFF_CONVERSION_FINISHED____OCR_PROCESSING, " +
                                         "'F.OCR_PROCESSING -> OCR_FINISHED', OCR_PROCESSING____OCR_FINISHED, " +
                                         "'G.OCR_FINISHED -> PRE_PROCESSING_FINISHED', OCR_FINISHED____PRE_PROCESSING_FINISHED, " +
                                         "'H.PRE_PROCESSING_FINISHED -> PIPELINE_PROCESSING', PRE_PROCESSING_FINISHED____PIPELINE_PROCESSING, " +
                                         "'I.PIPELINE_PROCESSING -> PIPELINE_FINISHED', PIPELINE_PROCESSING____PIPELINE_FINISHED, " +
                                         "'J.PIPELINE_FINISHED -> POST_PROCESSING', PIPELINE_FINISHED____POST_PROCESSING, " +
                                         "'K.POST_PROCESSING -> DOCUMENT_PROCESSED', POST_PROCESSING____DOCUMENT_PROCESSED " +
                                         ") as (PIPELINE, Time)"))

#"Time" represents the name of a column which contains numerical values
aggColumn = "Time"

print("Task 1:\nProcess all files and present a statistic about the runtimes per pipeline step (min, max, 10%/50%/90% percentile and mean).")
pipelineStatsDF.groupBy("PIPELINE").agg(
    min(aggColumn).alias("min"),
    max(aggColumn).alias("max"),
    mean(aggColumn).alias("mean"),
    expr(f"percentile({aggColumn}, 0.1)").alias("percentile10"),
    expr(f"percentile({aggColumn}, 0.5)").alias("percentile50"),
    expr(f"percentile({aggColumn}, 0.9)").alias("percentile90"))\
    .orderBy("PIPELINE")\
    .show(100, False)
# .select("PIPELINE",
#             "min",
#             "max",
#             "mean",
#             ceil("percentile10").alias("percentile10"),
#             ceil("percentile50").alias("percentile50"),
#             ceil("percentile90").alias("percentile90"))\

print("Task 2a:\nShow the top 5 slowest/fastest IDs per processing step. (Implement as switch via commandline option)")
#Allows to perform operations over a set of rows,window specification is set to partition the data by the "PIPELINE" column
windowSpec = Window.partitionBy("PIPELINE")


try:
    if args.option == "fastest":
        # It calculates the rank of the pipelines based on their Time column in ascending order
        # Filters the data to only show the top 5 pipelines with the lowest Time
        top_pipeline_df = pipelineStatsDF \
            .withColumn("rank", rank().over(windowSpec.orderBy(col("Time")))) \
            .where("rank < 6") \
            .orderBy(col("PIPELINE"), col("Time"))
    elif args.option == "slowest":
        # It calculates the rank of the pipelines based on their Time column in descending order
        # Filters the data to only show the top 5 pipelines with the Highest Time
        top_pipeline_df = pipelineStatsDF \
            .withColumn("rank", rank().over(windowSpec.orderBy(col("Time").desc()))) \
            .where("rank < 6") \
            .orderBy(col("PIPELINE"), col("Time").desc())
    else:
        raise ValueError(f"Invalid option: {option}")
except Exception as e:
    print(f"An unknown error occurred while processing the {option} pipeline: {e}")
finally:
    top_pipeline_df.show(100, False)

print("Task 2b:\nShow the total processing time(ms) between the states: PRE_PROCESSING -> PIPELINE_FINISHED")
# It creates a new column "PROCESSING_TIME" by subtracting the value in the "PRE_PROCESSING" column from the value in the
# "PIPELINE_FINISHED" column
metricDF\
 .select("ID", "PRE_PROCESSING", "PIPELINE_FINISHED")\
 .withColumn("PROCESSING_TIME", col("PIPELINE_FINISHED") - col("PRE_PROCESSING"))\
 .show(100, False)

print("Total time taken to execute the code: {:.2f} seconds".format(time.time() - start_time))

#Processing time in minutes: 60712/60,000 = 1.0118666666666667 minutes
# metricDF.select("ID", "PRE_PROCESSING", "PIPELINE_FINISHED")\
#         .withColumn("PROCESSING_TIME", (col("PIPELINE_FINISHED") - col("PRE_PROCESSING")))\
#         .withColumn("PROCESSING_TIME", divide(col("PROCESSING_TIME"), lit(60000)))\
#         .show(100, False)
#print(metricDF.count())
# metricDF.select("ID").filter(col("ID").isNotNull()).count()
