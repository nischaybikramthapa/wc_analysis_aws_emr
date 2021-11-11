package edu.rmit.cosc2367.s3819491.WordCountAssignment.TaskFourth;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

class WordCountPartitioner extends Partitioner <Text, LongWritable > {
	
	private static final Logger LOG = Logger.getLogger(WordCountPartitioner.class);

	@Override
	public int getPartition(Text key, LongWritable value, int numPartitions) {
		//set log level to debug
		LOG.setLevel(Level.DEBUG);

		String str = value.toString();

		if (numPartitions == 0) {
			LOG.debug("No partitioning - only One Reducer");
			return 0;
		}

		if (key.toString().equals("Extra Long") || key.toString().equals("Short")) {
			LOG.debug(str + " with the length of " + str.length() + " is directed to Partition: 0");
			return 0;
		} else {
			LOG.debug(str + " with the length of " + str.length() + " is directed to Partition: " + Integer.toString(1 % numPartitions));
			return 1 % numPartitions;
		}
	}
	
}