package edu.rmit.cosc2367.s3819491.WordCountAssignment.TaskTwo;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


public class WordCountByFirstCharacterMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	final private static LongWritable ONE = new LongWritable(1);
	private Text tokenValue = new Text();
	private static final Logger LOG = Logger.getLogger(WordCountByFirstCharacterMapper.class);


	@Override
	protected void map(LongWritable offset, Text text, Context context) throws IOException, InterruptedException {

		LOG.setLevel(Level.DEBUG);
		LOG.debug("The mapper task of Nischay Bikram Thapa, s3819491");

  	    //Split line into tokens
		StringTokenizer itr = new StringTokenizer(text.toString());

		while (itr.hasMoreTokens()) {
			String temptoken = itr.nextToken();
			if( startsWithVowel(temptoken) ) {

				tokenValue.set("Vowels");

			} else {
				tokenValue.set("Consonants");
			}
			
			context.write(tokenValue, ONE);
		}
		
	}
	
	public boolean startsWithVowel(String value) {
		if( value.startsWith("a") || value.startsWith("e") ||
			value.startsWith("i") || value.startsWith("o") ||
			value.startsWith("u") || value.startsWith("A") || 
			value.startsWith("E") || value.startsWith("I") ||
			value.startsWith("O") || value.startsWith("U")) {
			
			return true;
		}
		
		return false;
		
	}
	
}
