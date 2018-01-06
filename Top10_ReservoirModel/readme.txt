For Q2,
mutualfriend.jar file is provided along with the source code. There are two versions of this program one with high memory use(inside the folder named "works fine but high mem use") and the other with much lower memory use(inside the folder named "Q2_final")

Run the command in the Hadoop terminal:
hadoop jar <path/to/mutualfriend.jar> <path/to/soc-LiveJournal1Adj.txt> <path/to/output>

soc-LiveJournal1Adj.txt file is assumed to be in input folder in HDFS.
This program automatically deletes the output folder if it already exits.

This program outputs the TOP 10 pairs with maximum number of mutual friends amongst all the pairs. It has a mapper class, reducer class and custom partitioner as required in the program.
It outputs the results in the required format.

The program has been tested on a single node cluster and the Output of that can be found in the "output" folder in "Q2_final" folder where the mutualfriend.jar resides.


PLEASE NOTE: mutualfriend.jar file is a runnable jar.