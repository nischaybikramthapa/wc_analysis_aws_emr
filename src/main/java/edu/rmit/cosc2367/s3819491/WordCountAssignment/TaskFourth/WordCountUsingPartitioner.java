package edu.rmit.cosc2367.s3819491.WordCountAssignment.TaskFourth;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


public class WordCountUsingPartitioner extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(WordCountUsingPartitioner.class);

	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new WordCountUsingPartitioner(), args));
	}


	public int run(String[] args) throws Exception {
		Configuration configuration = getConf();
		//initialize MapReduce job
		Job job = new Job(configuration, "Word Count Using Partitioner");
	    //Set Map Reduce main jobconf class
		job.setJarByClass(WordCountUsingPartitioner.class);
		//set mapper
		job.setMapperClass(WordCountPartitionerMapper.class);
		//set map outputkeyclass
		job.setMapOutputKeyClass(Text.class);
		//set map output value
		job.setMapOutputValueClass(LongWritable.class);
		//set partitioner class
		job.setPartitionerClass(WordCountPartitioner.class);
		//set reducer class
		job.setReducerClass(WordCountPartitionerReducer.class);
		//set number of reduce tasks
		job.setNumReduceTasks(2);
		//set input format to text
		job.setInputFormatClass(TextInputFormat.class);
		//set output format to text
		job.setOutputFormatClass(TextOutputFormat.class);
		//set output key class
		job.setOutputKeyClass(Text.class);
		//set output value class
		job.setOutputValueClass(LongWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args [1]));

		LOG.info("Input path: " + args[0]);
		LOG.info("Output path: " + args[1]);

		return job.waitForCompletion(true) ? 0 : -1;
	}

}
