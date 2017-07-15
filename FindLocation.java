package findLocation;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import org.apache.log4j.BasicConfigurator;


public class FindLocation {
	//private static Logger logger = Logger.getLogger(AvgScore.class);
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
		throws IOException {
		try {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line, "\n");
			
			while (tokenizer.hasMoreElements()) {
				StringTokenizer tokenizerForLine = new StringTokenizer(tokenizer.nextToken());
				String value0 = tokenizerForLine.nextToken();
				String value1 = tokenizerForLine.nextToken();
				if (value0.charAt(0) >= '0' && value0.charAt(0) <= '9') {
					output.collect(new Text(value0), new Text("Location" + " " + value1));
					//System.out.println("Location" + " " + value1);
				}else {
					output.collect(new Text(value1), new Text("User" + " " + value0));
					//System.out.println("User" + " " + value0);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		}
	}

	public static class Reduce extends MapReduceBase implements 
	Reducer<Text, Text, Text, Text> 
	{
		public void reduce(Text key, Iterator<Text> values, 
				OutputCollector<Text, Text> output, Reporter reporter)
		throws IOException 
		{
		try {
			String[] users = new String[5];
			String[] locations = new String[5];
			int userCounter = 0;
			int locationCounter = 0;
			
			while (values.hasNext()) {
				String line = values.next().toString();
				StringTokenizer tokenizer = new StringTokenizer(line);
				String userOrLocation = tokenizer.nextToken();
				String currValue = tokenizer.nextToken();

				if(userOrLocation.equals("Location")) {
					locations[locationCounter] = currValue;
					locationCounter ++;
				}else {
					users[userCounter] = currValue;
					userCounter ++;
				}
			}
			
			if (userCounter > 0 && locationCounter >0) {
				for (int i=0; i<userCounter; i++) {
					for (int j=0; j<locationCounter; j++) {
						output.collect(new Text(users[i]), new Text(locations[j]));
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		}
	}
	
	public static void main(String[] args) throws Exception {
		
		BasicConfigurator.configure();
		JobClient client = new JobClient();
		JobConf job = new JobConf(FindLocation.class);

		job.setJobName("FindLocation");
		job.set("fs.default.name", "hdfs://192.168.245.128:9000");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(Map.class);
		//job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		
		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.245.128:9000//findLocation"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.245.128:9000//findLocationOutput"));
		client.setConf(job);
		JobClient.runJob(job);


	}
}
