package edu.rmit.cosc2367.s3819491.WordCountAssignment.TaskTwo;

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

import edu.rmit.cosc2367.s3819491.WordCountAssignment.TaskOne.WordCountByLengthReducer;

public class WordCountByFirstCharacter extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(WordCountByFirstCharacter.class);


	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new WordCountByFirstCharacter(), args));
	}


	public int run(String[] args) throws Exception {
		Configuration configuration = getConf();
	    
		//Initialising Map Reduce Job
		Job job = new Job(configuration, "Word Count By Vowel and Consonant");
		
	    //Set Map Reduce main jobconf class
		job.setJarByClass(WordCountByFirstCharacter.class);

	    //Set Mapper class
		job.setMapperClass(WordCountByFirstCharacterMapper.class);
		
	     //Set Combiner class
		job.setCombinerClass(WordCountByFirstCharacterReducer.class);
		
	    //set Reducer class
		job.setReducerClass(WordCountByFirstCharacterReducer.class);

	    //set Input Format
		job.setInputFormatClass(TextInputFormat.class);
		
	    //set Output Format
		job.setOutputFormatClass(TextOutputFormat.class);

		LOG.setLevel(Level.INFO);

	    //set Output key class

		job.setOutputKeyClass(Text.class);
		
		//set Output value class

		job.setOutputValueClass(LongWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args [1]));

		LOG.info("Input path: " + args[0]);
		LOG.info("Output path: " + args[1]);

		return job.waitForCompletion(true) ? 0 : -1;
	}

}
