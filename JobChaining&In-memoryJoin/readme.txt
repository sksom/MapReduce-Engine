For Q4,
yelpstatistics.jar file is provided along with the source code.

Run the command in the Hadoop terminal:
hadoop jar <path/to/yelpstatistics.jar> <path/to/business.csv> <path/to/review.csv> <path/to/output>

review.csv and business.csv file is assumed to be in input folder in HDFS.
This program automatically deletes the output folder if it already exits.

This program outputs the 'user id' and 'rating' of users that reviewed businesses located in “Palo Alto”. It has a mapper class and a reducer class as required in the program for in-memory join and distributed cache use.
It outputs the results in the required format.

The program has been tested on a single node cluster and the Output of that can be found in the "output" folder in "Q4_final" folder where the yelpstatistics.jar resides.


PLEASE NOTE: yelpstatistics.jar file is a runnable jar.