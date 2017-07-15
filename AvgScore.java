package avgScore;

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

//import org.apache.log4j.Logger;

public class AvgScore {
	//private static Logger logger = Logger.getLogger(AvgScore.class);
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
		throws IOException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line, "\n");
			
			while (tokenizer.hasMoreElements()) {
				StringTokenizer tokenizerForLine = new StringTokenizer(tokenizer.nextToken());
				String studentName = tokenizerForLine.nextToken();
				String subjectName = tokenizerForLine.nextToken();
				String score       = tokenizerForLine.nextToken();
				
				Text name = new Text(studentName);
				int scoreInt = Integer.parseInt(score);
				output.collect(name, new IntWritable(scoreInt));
			}
		}
	}

	public static class Reduce extends MapReduceBase implements 
	Reducer<Text, IntWritable, Text, IntWritable> 
	{
		public void reduce(Text key, Iterator<IntWritable> values, 
				OutputCollector<Text, IntWritable> output, Reporter reporter)
		throws IOException 
		{
			int scoreSum = 0;
			int subjectCounter = 0;
			
			while (values.hasNext()) {
				scoreSum += values.next().get();
				subjectCounter ++;
			}
			int scoreAvg = (int) scoreSum / subjectCounter;
			output.collect(key, new IntWritable(scoreAvg));
		}
	}
	
	public static void main(String[] args) throws Exception {
		
		BasicConfigurator.configure();
		JobClient client = new JobClient();
		JobConf job = new JobConf(AvgScore.class);

		job.setJobName("AvgScore");
		job.set("fs.default.name", "hdfs://192.168.245.128:9000");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		
		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.245.128:9000//scoreAvg"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.245.128:9000//scoreAvgOutput"));
		client.setConf(job);
		JobClient.runJob(job);


	}
}
