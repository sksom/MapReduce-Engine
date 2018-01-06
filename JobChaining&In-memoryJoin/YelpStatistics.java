import java.io.IOException;
import java.util.*;
import java.net.URI;
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class YelpStatistics{
	//Mapper class
	public static class MFMap extends Mapper<LongWritable,Text,Text,Text>{
		
		private HashMap<String, String> tm = new HashMap<String, String>();

		@Override
		protected void setup(Context context) throws IOException,InterruptedException {

			URI[] files = context.getCacheFiles();	//there can be multiple cache files, our case we have only one cache file

			if (files.length == 0) 		// cache files not found
			{
				throw new FileNotFoundException("Distributed cache file not found");
			}
			
			FileSystem fs = FileSystem.get(context.getConfiguration());
			FSDataInputStream in = fs.open(new Path(files[0]));
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			readCacheFile(br);		//custom function to read cache file
		}

		private void readCacheFile(BufferedReader br) throws IOException {
			
			//split the input file(business.csv) into individual lines and split individual line into values separated by ::
			String dataLine = br.readLine();
			while (dataLine != null) 
			{
				String[] fields = dataLine.split("::");
				if(fields[1].contains("Palo Alto"))
				{
					tm.put(fields[0], "Palo Alto");		// putting cache file data into hashmap for ease
				}
				dataLine = br.readLine();
			}
		}
		
		

		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {

			//split the input file(review.csv) into individual lines and split individual line into values separated by ::
			String[] dataLine=value.toString().split("::");
			if(tm.containsKey(dataLine[2]))
			{
				context.write(new Text(dataLine[1]), new Text(dataLine[3])); //emit the pair
			}
		}
	}


	//Reducer class
	public static class MFReduce extends Reducer<Text,Text,Text,Text>{

		@Override
		protected void reduce(Text user_id, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)throws IOException, InterruptedException {
			String star_rating = "";
			for (Text value : values) 
			{
				star_rating=value.toString();
				context.write(user_id, new Text(star_rating));
			}
		}
	}
	
	
	//Driver function
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		//this statement will take the arguments after you specify your main() function class in CLI i.e. here HDFS input directory and HDFS output directory
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		//Check below whether user enter all 3 args or not
		if (otherArgs.length != 3) {
			System.err.println("Usage: Mutual friend <fileToBeCached> <in> <out>");
			System.exit(2);
		}
		
		//often HDFS output directory already exists if we have run the HADOOP before then either specify new folder in CLI or include below in ur program
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(args[2]))) {
			// If exist delete the output path
			fs.delete(new Path(args[2]), true);
		}
		
		Path inputFile1 = new Path(otherArgs[0]);		//to be cached
		Path inputFile2 = new Path(otherArgs[1]);		//real input file
		Path outputFile = new Path(otherArgs[2]);		//output file
		
		//create the job and set the respective classes
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "yelpstatistics");
		job.setJarByClass(YelpStatistics.class);
		job.setMapperClass(MFMap.class);
		job.setReducerClass(MFReduce.class);
		job.addCacheFile(inputFile1.toUri());
		
		//set output key type
		job.setOutputKeyClass(Text.class);
		//set output value type
		job.setOutputValueClass(Text.class);
		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job, inputFile2);
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job,outputFile);
		
		// Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);	
	}
}



