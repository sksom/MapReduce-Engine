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
		
		//hashmap to save top 10 entries
		private HashMap<String, Integer> tm = new HashMap<String, Integer>();
		
		public void reduce(Text key,Iterable<IntWritable> value, Context context) throws IOException, InterruptedException {
			String strTemp=key.toString();	// because we want key (user1,user2) as a string to be saved into a treemap as value not key
			
			//since reducer will automatically pack key,values[v1,v2,v3...]
			//here we only need to sum up the pair's values we will receive from key
			int sum=0;
			
			for(IntWritable keyPairValue:value){
				sum+=keyPairValue.get();
			}
			//Convert key into string and along with sum as value save both into the treemap tm
			
			//tm.put(new IntWritable(sum), strTemp);
			tm.put(strTemp,sum);
			//till here i have hashmap of all values
			//now all i need to do is output max 10 out of it in clean up
		}
		
		@Override
		protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)throws IOException, InterruptedException {
			
			Map<String, Integer> sortedMap = new TreeMap<String, Integer>(new ValueComparator(tm));
			sortedMap.putAll(tm);
			int i = 0;
			for (Map.Entry<String, Integer> entry : sortedMap.entrySet()) {
				context.write(new Text(entry.getKey()),new IntWritable(entry.getValue()));
				i++;
				if (i == 10)
					break;
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
class ValueComparator implements Comparator<Object> {
	 
	Map<String, Integer> map;

	public ValueComparator(Map<String, Integer> map) {
		this.map = map;
	}
	public int compare(Object keyA, Object keyB) {
		
		Integer valueA= (Integer) map.get(keyA);
		Integer valueB= (Integer) map.get(keyB);
		
		int compare=valueB.compareTo(valueA);
		
		if(compare==0)
			return 1;		
		return compare;
	}
}