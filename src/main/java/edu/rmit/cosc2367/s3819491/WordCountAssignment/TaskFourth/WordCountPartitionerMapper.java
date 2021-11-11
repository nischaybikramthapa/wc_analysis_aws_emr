package edu.rmit.cosc2367.s3819491.WordCountAssignment.TaskFourth;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


class WordCountPartitionerMapper extends Mapper<LongWritable,Text,Text,LongWritable> {
	private static final Logger LOG = Logger.getLogger(WordCountPartitionerMapper.class);

	/**
	 * The input parameters of this method are those key value pairs extracted from the input files( recall pics on lecture slides)
	 * based on the rule we previously specified.
	 *
	 * @param offset The number of characters before the first character of current line.
	 * @param text The content of the line.
	 * @param context A bridge streams output of your mapper algorithm to the framework that will pass it to Reducer nodes later.
	 */
	@Override
	protected void map(LongWritable key, Text text, Context context) throws IOException, InterruptedException {

		// Set log-level to debugging
		LOG.setLevel(Level.DEBUG);
		LOG.debug("The mapper task of Nischay Bikram Thapa, s3819491");
		Text word = new Text();
		StringTokenizer itr = new StringTokenizer(text.toString());
		try {
			// Log every line
			LOG.debug(text);
			
			while (itr.hasMoreTokens()) {
				
				String token = itr.nextToken();
				
				if(token.length() > 10 ) {

					word.set("Extra Long"); 

				} else if (token.length() >= 8 && token.length() <= 10) {

					word.set("Long");

				}  else if (token.length() >= 5 && token.length() <= 7) {

					word.set("Medium");

				} else if (token.length() >= 1 && token.length() <= 4) {

					word.set("Short");

				} else {
					
					word.set("UnKnown");
					
				}

				context.write(word, new LongWritable(1));

			}
		}
		catch (Exception ex) {
			LOG.error("Caught Exception", ex);
		}
	}
}
