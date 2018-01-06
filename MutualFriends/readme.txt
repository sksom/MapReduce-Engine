For Q1,
mutualfriend.jar file is provided along with the source code.

Run the command in the Hadoop terminal:
hadoop jar <path/to/mutualfriend.jar> <path/to/soc-LiveJournal1Adj.txt> <path/to/output> <User1> <User2>

soc-LiveJournal1Adj.txt file is assumed to be in input folder in HDFS.
This program automatically deletes the output folder if it already exits.

This program outputs the common friends of input pair given as the arguments. It has a mapper class, reducer class and custom partitioner as required in the program.
It outputs the results in the required format.

The program has been tested on a single node cluster and the Output of that can be found in the "output" folder in "Q1_final" folder where the mutualfriend.jar resides.


PLEASE NOTE: mutualfriend.jar file is a runnable jar.