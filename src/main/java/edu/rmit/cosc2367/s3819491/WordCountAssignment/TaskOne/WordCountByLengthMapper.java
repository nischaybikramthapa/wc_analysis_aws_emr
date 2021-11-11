package edu.rmit.cosc2367.s3819491.WordCountAssignment.TaskOne;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;



public class WordCountByLengthMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	final private static LongWritable ONE = new LongWritable(1);
	private Text tokenValue = new Text();
	private static final Logger LOG = Logger.getLogger(WordCountByLengthMapper.class);


	@Override
	protected void map(LongWritable offset, Text text, Context context) throws IOException, InterruptedException {

        LOG.setLevel(Level.DEBUG);
		LOG.debug("The mapper task of Nischay Bikram Thapa, s3819491");

        StringTokenizer itr = new StringTokenizer(text.toString());
        
		while (itr.hasMoreTokens()) {
			
			String token = itr.nextToken();
			
			if(token.length() > 10 ) {

				tokenValue.set("Extra Long"); 

			} else if (token.length() >= 8 && token.length() <= 10) {

				tokenValue.set("Long");

			}  else if (token.length() >= 5 && token.length() <= 7) {

				tokenValue.set("Medium");

			} else if (token.length() >= 1 && token.length() <= 4) {

				tokenValue.set("Short");

			} else {
				
				LOG.debug("Cannot find length for" + token);
				tokenValue.set("Unknown");

			}

			context.write(tokenValue, ONE);

		}
	}
}
