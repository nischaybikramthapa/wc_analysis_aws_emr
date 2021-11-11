package edu.rmit.cosc2367.s3819491.WordCountAssignment.TaskThree;

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



public class InMapperWordCount extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(InMapperWordCount.class);


	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new InMapperWordCount(), args));
	}


	public int run(String[] args) throws Exception {
		Configuration configuration = getConf();
	    //Initialising Map Reduce Job

		Job job = new Job(configuration, "Word Count Using In Mapper");
		
	    //Set Map Reduce main jobconf class
		job.setJarByClass(InMapperWordCount.class);

	    //Set Mapper class
		job.setMapperClass(InMapperWordCountMapper.class);
		
		//set reducer class
		job.setReducerClass(InMapperWordCountReducer.class);

		//set input format
		job.setInputFormatClass(TextInputFormat.class);
		
		//set output format
		job.setOutputFormatClass(TextOutputFormat.class);

		LOG.setLevel(Level.INFO);

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
