import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class MutualFriend{
	//Mapper class
	public static class MFMap extends Mapper<LongWritable,Text,Text,Text>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//need to parse the command line arguments inputs i.e. usr1 and usr2
			Configuration conf = context.getConfiguration();
			String usrArgInput1 = conf.get("usr1");
			String usrArgInput2 = conf.get("usr2");
			//split the input file into individual lines and further the individual lines into their user and friendsList
			String[] dataLine=value.toString().split("\\t");
			
			//Proceed if person entry [0] of the dataLine matches either user supplied as argument
			if(dataLine[0].compareTo(usrArgInput1)==0 || dataLine[0].compareTo(usrArgInput2)==0)	//it means, if our dataLine contains the user supplied at CLI argument as first entry in line
			{
				Text mapValue=new Text();
				Text mapKey=new Text();
				mapValue.set(dataLine[1]);		//Now it contains all comma separated friends of entry dataLine[0]
				
				//since we need to make pair of dataLine[0] with each comma separated friend we will require a list of all friends as individual entry
				List<String> friendsList= new ArrayList<>();
				String[] friendsArray;
				if(dataLine.length>1)		//i.e. atleast the person has one friend
				{
					friendsArray=dataLine[1].split(",");
					for(String f:friendsArray)
					{
						friendsList.add(f);
					}
				}
				//till this point i have all the data saved tht mapper wants to emit
				
				//arranging data in emitting format ((pair),(friends))
				String k1="";;
				String k2=dataLine[0];
				for(String k:friendsList)
				{
					k1="";
					if (k.compareTo(k2) > 0)
					{
						k1=k2+","+k;
					}
					else
					{
						k1=k+","+k2;
					}
					mapKey.set(k1);
					context.write(mapKey,mapValue);
				}
			}
		}
	}
	
	//Partitioner class
	public static class MyPartition extends Partitioner<Text,Text>{
		public int getPartition(Text key, Text value, int numPartitions) {
			String[] temp=key.toString().split(",");
			String s = temp[0] + "9999999" + temp[1];
			return s.hashCode() % numPartitions;
		}
	}
	
	
	//Reducer class
	public static class MFReduce extends Reducer<Text,Text,Text,Text>{
		private Text outputText = new Text();
		public void reduce(Text key,Iterable<Text> value, Context context) throws IOException, InterruptedException {
			String strTemp[]=new String[2];	// why 2? because a 1 key has only two associated lists of friends, first from user1 and second from user2 in key (user1,user2)
			int count=0;
			
			//since reducer will automatically pack key,values[v1,v2,v3...]
			if (value.iterator().hasNext()) 
			{
				strTemp[0] = value.iterator().next().toString();
				count = 1;
			}
			if(value.iterator().hasNext())
			{
				strTemp[1] = value.iterator().next().toString();
				count = 2;
			}
			if(count>1)
			{
				//String arrays for saving splitted values
				String[] s1=strTemp[0].split(",");
				String[] s2=strTemp[1].split(",");
				//Convert these string array to list for easy manipulation
				List<String> l1= new ArrayList<>();	//for s1
				List<String> l2= new ArrayList<>(); //for s2
				List<String> intersectionList= new ArrayList<>(); //for saving intersection of s1 and s2
				for (String s : s1) 
				{
					l1.add(s);
				}
				for (String s : s2) 
				{
					l2.add(s);
				}
				//finding intersection of the two lists
				if (l1.size() > l2.size()) {
					l1.retainAll(l2);
					intersectionList = l1;
				} else {
					l2.retainAll(l1);
					intersectionList = l2;
				}
				
				//convert this list intersectionList into a string in which values are separated with commas bcz tht's wht we have to output
				String lastString = "";

				for (int i = 0; i < intersectionList.size(); i++) {
					if (i == 0) 
					{
						lastString = lastString + intersectionList.get(0);
					} 
					else {
						lastString = lastString + "," + intersectionList.get(i);
					}

				}
				outputText.set(lastString);  // lastString converted to Text object to be given as output
				//Produce the final output
				context.write(key, outputText);
			}
		}
	}
	
	
	//Driver function
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		//this statement will take the arguments after you specify your main() function class in CLI i.e. here HDFS input directory, HDFS output directory, usr1 and usr2
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		//Check below whether user enter all 4 args or not
		if (otherArgs.length != 4) {
			System.err.println("Usage: Mutual friend <in> <out> <User1> <User2>");
			System.exit(2);
		}
		
		//often HDFS ouput directory already exists if we have run the HADOOP before then either specify new folder in CLI or include below in ur program
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(args[1]))) {
			// If exist delete the output path
			fs.delete(new Path(args[1]), true);
		}
		
		//set the values for usr1 and usr2 to be used in mapper
		conf.set("usr1", otherArgs[2]);
		conf.set("usr2", otherArgs[3]);
		
		//create the job and set the respective classes
		Job job = new Job(conf, "mutualfriend");
		job.setJarByClass(MutualFriend.class);
		job.setMapperClass(MFMap.class);
		job.setReducerClass(MFReduce.class);
		
		//set partitioner class
		job.setPartitionerClass(MyPartition.class);
		
		//set output key type
		job.setOutputKeyClass(Text.class);
		//set output value type
		job.setOutputValueClass(Text.class);
		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		// Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);	
	}
}