For Q3,
yelpstatistics.jar file is provided along with the source code.

Run the command in the Hadoop terminal:
hadoop jar <path/to/yelpstatistics.jar> <path/to/review.csv> <path/to/business.csv> <path/to/output>

review.csv and business.csv file is assumed to be in input folder in HDFS.
This program automatically deletes the output folder if it already exits.

This program outputs List the business_id, full address, categories and average rating of the Top 10 businesses using the average ratings. It has 2 mapper classes and 2 reducer classes as required in the program for job chaining.
It outputs the results in the required format.

The program has been tested on a single node cluster and the Output of that can be found in the "output" folder in "Q3_final" folder where the yelpstatistics.jar resides.


PLEASE NOTE: yelpstatistics.jar file is a runnable jar.