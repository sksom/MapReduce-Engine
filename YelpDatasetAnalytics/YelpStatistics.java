import java.io.IOException;
import java.util.*;
import java.util.Comparator;
import java.util.Map;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;


public class YelpStatistics{
	//Mapper class
	public static class MFMap extends Mapper<LongWritable,Text,Text,DoubleWritable>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			//split the input file(review.csv) into individual lines and split individual line into values separated by ::
			String[] dataLine=value.toString().split("::");
			
			DoubleWritable mapValue= new DoubleWritable();
			double tempmv=Double.parseDouble(dataLine[3]);	// star rating saved
			mapValue.set(tempmv);
			
			Text mapKey=new Text();
			String tempmk=dataLine[2];
			mapKey.set(tempmk);		// corresponding business_id saved
			
			context.write(mapKey,mapValue);
		}
	}
	
	//Reducer class
	public static class MFReduce extends Reducer<Text,DoubleWritable,Text,DoubleWritable>{
		//hashmap to save top 10 entries
		private HashMap<String, Double> tm = new HashMap<String, Double>();
		
		public void reduce(Text key,Iterable<DoubleWritable> value, Context context) throws IOException, InterruptedException {
			
			//since reducer will automatically pack key,values[v1,v2,v3...]
			//here we only need to sum up the pair's values we will receive from key
			double sum=0;
			int count=0;
			
			for(DoubleWritable keyPairValue:value){
				sum+=keyPairValue.get();
				count++;
			}
			
			double avg=sum/count;	//average rating
			tm.put(key.toString(), avg);
		}
		
		@Override
		protected void cleanup(Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context)throws IOException, InterruptedException {
			
			Map<String, Double> sortedMap = new TreeMap<String, Double>(new ValueComparator(tm));
			sortedMap.putAll(tm);
			int i = 0;
			for (Map.Entry<String, Double> entry : sortedMap.entrySet()) {
				context.write(new Text(entry.getKey()),new DoubleWritable(entry.getValue()));
				i++;
				if (i == 10)
					break;
			}
		}
	}
	
	
	/*---------------------------------Phase change-----------------------------------------*/
	//Above this limit everything done till this point was to obtain top 10 businesses with max. average ratings.
	//From this point onwards we will write our second reducer that'll perform reducer side join.


	//REDUCER SIDE JOIN begins

	//Now I need two mappers, one for processing intermediate file we receive and one for processing the new business.csv file

	//mapper for intermediate file
	public static class MapperForIntermediateFile extends Mapper<LongWritable, Text, Text, Text> {
		
		@Override
		protected void map(LongWritable key, Text value,Mapper<LongWritable, Text, Text, Text>.Context context)throws IOException, InterruptedException {
			
			if(value.getLength()>0)		//EOF not reached, treat intermediate file as collection of multiple lines(here 10) in the form, Line: business_id \t avg_rating
			{							//whole full line is a value as typically happens in the mapper
				String[] fields=value.toString().split("\t");	
				String tempvalue= "a:\t"+fields[1];				//appending any random identifier for identifying the rating
				context.write(new Text(fields[0].trim()), new Text(tempvalue));
			}
		}
	}

	//mapper for business.csv file
	public static class MapperForSecondFile extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {

			//split the input file(business.csv) into individual lines and split individual line into values separated by ::
			String[] dataLine=value.toString().split("::");
			
			Text business_id = new Text();
			business_id.set(dataLine[0]);
			
			Text details = new Text();
			String  tempvalue=dataLine[1]+"\t"+dataLine[2];
			details.set(tempvalue);
			
			//emit the pair
			context.write(business_id, details);
		}
	}


	//Final reducer is required now
	public static class Final_Reducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text business_id, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)throws IOException, InterruptedException {
			
			boolean flag = false;

			String star_rating = "";
			String other_details = "";
			for (Text value : values) 
			{
				if (value.toString().contains("a:")) 
				{
					String[] tp= value.toString().split("\t");
					star_rating = tp[1];
					flag = true;
				} else
					other_details = value.toString();
			}
			if (!flag)
				return;		//only emit when there is corresponding rating value, else it's just a useless pair for us so return and do nothing

			String outputValue = other_details + "\t" + star_rating;
			context.write(business_id, new Text(outputValue));
		}
	}
	
	
	//Driver function
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		//this statement will take the arguments after you specify your main() function class in CLI i.e. here HDFS input directory and HDFS output directory
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		//Check below whether user enter all 3 args or not
		if (otherArgs.length != 3) {
			System.err.println("Usage: Mutual friend <in1> <in2> <out>");
			System.exit(2);
		}
		
		//often HDFS output directory already exists if we have run the HADOOP before then either specify new folder in CLI or include below in ur program
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(args[2]))) {
			// If exist delete the output path
			fs.delete(new Path(args[2]), true);
		}
		
		
		Path inputFile1 = new Path(otherArgs[0]);
		Path inputFile2 = new Path(otherArgs[1]);
		Path outputFile = new Path(otherArgs[2]);
		Path intermidiateFile = new Path("input/intermediate_data");
		
		//create the job and set the respective classes
		@SuppressWarnings("deprecation")
		Job job1 = new Job(conf, "yelpstatistics_sub");
		job1.setJarByClass(YelpStatistics.class);
		job1.setMapperClass(MFMap.class);
		job1.setReducerClass(MFReduce.class);
		
		//set output key type
		job1.setOutputKeyClass(Text.class);
		//set output value type
		job1.setOutputValueClass(DoubleWritable.class);
		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job1, inputFile1);
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job1, intermidiateFile);
		
		// Wait till job1 completion
		job1.waitForCompletion(true);
		
		
		/*----------------------------------------------------------------------------------*/
		//Job2
		@SuppressWarnings("deprecation")
		Job job2 = new Job(conf, "yelpstatistics");
		job2.setJarByClass(YelpStatistics.class);

		job2.setReducerClass(Final_Reducer.class);

		MultipleInputs.addInputPath(job2, intermidiateFile,
				TextInputFormat.class, MapperForIntermediateFile.class);
		MultipleInputs.addInputPath(job2, inputFile2, TextInputFormat.class,
				MapperForSecondFile.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job2, outputFile);
		job2.waitForCompletion(true);
		
		FileSystem fsfnl = FileSystem.get(conf);
		fsfnl.delete(intermidiateFile, true);	//delete intermediate file after job2 finishes.
		// Wait till final job2 completion
		System.exit(job2.waitForCompletion(true) ? 0 : 1);	
	}
}

//below class is to sort the values in a Map in ascending order
class ValueComparator implements Comparator<Object> {
	 
	Map<String, Double> map;
 
	public ValueComparator(Map<String, Double> map) {
		this.map = map;
	}
	public int compare(Object keyA, Object keyB) {
		
		Double valueA= (Double) map.get(keyA);
		Double valueB= (Double) map.get(keyB);
		
		int compare=valueB.compareTo(valueA);
		
		if(compare==0)
			return 1;		
		return compare;
	}
}



