import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class MutualFriend{
	//Mapper class
	public static class MFMap extends Mapper<LongWritable,Text,Text,IntWritable>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			//split the input file into individual lines
			String[] dataLine=value.toString().split("\\t");
			
			IntWritable mapValue= new IntWritable();
				
			Text mapKey=new Text();
				
			//since we need to make pair of dataLine[0] with each comma separated friend we will require a list of all friends as individual entry
			List<String> friendsList= new ArrayList<>();
			String[] friendsArray;
			if(dataLine.length>1)		//i.e. at least the person has one friend
			{
				friendsArray=dataLine[1].split(",");
				for(String f:friendsArray)
				{
					friendsList.add(f);
				}
			}
			//till this point i have all the data saved tht mapper wants to emit
			
			//arranging data in emitting format ((pair),count)
			int count=0;
			String k1="";
			String k2=dataLine[0];		//source
			for(String k:friendsList)
			{
				k1="";
				//below conditions order the pair lexicographically
				if (k.compareTo(k2) > 0)
				{
					k1=k2+","+k;
					count=0;
				}
				else
				{
					k1=k+","+k2;
					count=0;
				}
				mapKey.set(k1);
				mapValue.set(count);
				context.write(mapKey,mapValue);
				//below loop emits pair within friends in friendsList
				for(String loopk:friendsList)
				{
					k1="";
					//below conditions order the pair lexicographically
					if(loopk.compareTo(k)==0)
					{
						continue;
					}
					else if (loopk.compareTo(k) > 0)
					{
						k1=k+","+loopk;
						count=1;
					}
					else
					{
						k1=loopk+","+k;
						count=1;
					}
					mapKey.set(k1);
					mapValue.set(count);
					context.write(mapKey,mapValue);
				}
			}
		}
	}
	
	//Partitioner class
	public static class MyPartition extends Partitioner<Text,IntWritable>{
		public int getPartition(Text key, IntWritable value, int numPartitions) {
			String[] temp=key.toString().split(",");
			String s = temp[0] + "9999999" + temp[1];
			return s.hashCode() % numPartitions;
		}
	}
	
	
	//Reducer class
	public static class MFReduce extends Reducer<Text,IntWritable,Text,IntWritable>{
		
		private int fcount=0;
		private boolean done=false;		//to check whether the tm is copied into sorted map or not
		private int tempValue=0;
		//this function returns sorted map on the basis of value
		public static TreeMap<String, Integer> sortMapByValue(HashMap<String, Integer> map)
		{
			//map is tm
			Comparator<String> comparator = new ValueComparator(map);
			//TreeMap is a map sorted by its keys. 
			//The comparator is used to sort the TreeMap by values here. 
			TreeMap<String, Integer> result = new TreeMap<String, Integer>(comparator);	//here we tell our treemap how to sort it's contents and then we populate our treemap
			result.putAll(map);
			return result;
		}
		//hashmap to save top 10 entries
		private HashMap<String, Integer> tm = new HashMap<String, Integer>();
		private TreeMap<String, Integer> sortedMap=new TreeMap<String, Integer>();
		
		public void reduce(Text key,Iterable<IntWritable> value, Context context) throws IOException, InterruptedException {
			String strTemp=key.toString();	// because we want key (user1,user2) as a string to be saved into a treemap as value not key
			
			//since reducer will automatically pack key,values[v1,v2,v3...]
			//here we only need to sum up the pair's values we will receive from key
			int sum=0;
			
			for(IntWritable keyPairValue:value){
				sum+=keyPairValue.get();
			}
			//Converted key into string above and along with sum as value save both into the treemap tm
			
			if(fcount<10)
			{
				tm.put(strTemp,sum);
				fcount++;
			}
			else		//after saving 10 values in tm else will be executed when 11th value is received
			{
				if(!done)
				{
					sortedMap = sortMapByValue(tm);	//sorted by value max. first
					done=true;
					
					//Get a pointer to the last(minimum) entry   
				    Map.Entry<String, Integer> me=(Map.Entry<String, Integer>)sortedMap.lastEntry();
				    tempValue=(int) me.getValue();		//last entry's value
				}
				
				if(tempValue<sum)
				{
					//Removing last(minimum) entry and then
					
				    //putting new entry along with all previous into a different Hashmap
				    HashMap<String, Integer> tm2 = new HashMap<String, Integer>();
				    int i = 0;
					for (Map.Entry<String, Integer> entry : sortedMap.entrySet()) {
						//i++;
						//if(i==1)
							//continue;
						tm2.put(new String(entry.getKey()),new Integer(entry.getValue()));
						i++;
						if (i == 9)
						{
							sortedMap.clear();
							break;
						}
					}
				    //after above loop my hashmap has nine top entries of sortedMap treemap and sortedMap is cleared
				    tm2.put(strTemp,sum);
				    //sorting hashmap again
				    sortedMap=sortMapByValue(tm2);
				    
				    //Get a pointer to the last(minimum) entry   
				    Map.Entry<String, Integer> me=(Map.Entry<String, Integer>)sortedMap.lastEntry();
				    tempValue=(int) me.getValue();		//last(minimum) entry's value
				}
				else
					return;
			}
			//till here i have a treemap with top 10 max. of all values
			//now all i need to do is output it in clean up
		}
		
		@Override
		protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)throws IOException, InterruptedException {
			//emit the entries of sorted map
			for (Map.Entry<String, Integer> entry : sortedMap.entrySet()) {
				context.write(new Text(entry.getKey()),new IntWritable(entry.getValue()));
			}
		}
	}
	
	
	//Driver function
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		//this statement will take the arguments after you specify your main() function class in CLI i.e. here HDFS input directory and HDFS output directory
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		//Check below whether user enter all 2 args or not
		if (otherArgs.length != 2) {
			System.err.println("Usage: Mutual friend <in> <out>");
			System.exit(2);
		}
		
		//often HDFS ouput directory already exists if we have run the HADOOP before then either specify new folder in CLI or include below in ur program
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(args[1]))) {
			// If exist delete the output path
			fs.delete(new Path(args[1]), true);
		}
		
		//create the job and set the respective classes
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "mutualfriend");
		job.setJarByClass(MutualFriend.class);
		job.setMapperClass(MFMap.class);
		job.setReducerClass(MFReduce.class);
		
		//set partitioner class
		job.setPartitionerClass(MyPartition.class);
		
		//set output key type
		job.setOutputKeyClass(Text.class);
		//set output value type
		job.setOutputValueClass(IntWritable.class);
		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		// Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);	
	}
}

//below class is to sort the values in a Map in ascending order
class ValueComparator implements Comparator<String>{
	 
	//temporarily storing the map passed as the argument to the constructor
	HashMap<String, Integer> map = new HashMap<String, Integer>();
 
	//constructor
	public ValueComparator(HashMap<String, Integer> map)
	{
		this.map.putAll(map);
	}
 
	@Override
	public int compare(String s1, String s2) 
	{
		//s1 and s2 are the keys of the two entries of the map to be compared
		//why i need to do this? bcz compare method by default will populate s1 and s2 with the keys of the map entries
		if(map.get(s1) > map.get(s2))	//map.get(s1) will resolve to it's corresponding Integer key, same for s2
		{
			return -1;
		}
		else if(map.get(s1) == map.get(s2))
		{
			return 0;
		}
		else
		{
			return 1;
		}	
		// 1 then 0 then -1 means increasing order and -1 then 0 then 1 means decreasing order
	}
}