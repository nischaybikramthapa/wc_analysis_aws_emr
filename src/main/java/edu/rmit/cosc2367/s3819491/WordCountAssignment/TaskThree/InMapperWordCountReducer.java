package edu.rmit.cosc2367.s3819491.WordCountAssignment.TaskThree;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


public class InMapperWordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

	private LongWritable total = new LongWritable();
	private static final Logger LOG = Logger.getLogger(InMapperWordCountReducer.class);

	@Override
	protected void reduce(Text token, Iterable<LongWritable> counts, Context context)
			throws IOException, InterruptedException {

		LOG.setLevel(Level.DEBUG);
		LOG.debug("The reducer task of Nischay Bikram Thapa, s3819491");

		long n = 0;
        //Calculate sum of counts
		for (LongWritable count : counts)
			n += count.get();
		total.set(n);

		context.write(token, total);
	}
}
