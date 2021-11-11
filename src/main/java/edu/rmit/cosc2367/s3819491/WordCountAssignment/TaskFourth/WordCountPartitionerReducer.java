package edu.rmit.cosc2367.s3819491.WordCountAssignment.TaskFourth;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class WordCountPartitionerReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
	
	private static final Logger LOG = Logger.getLogger(WordCountPartitionerReducer.class);
	public int max = -1;

	/**
	 * After mapping, the pairs are streamed to reducers, in which the reduce algorithm are executed.
	 *
	 * @param token The key of the output pair from Mapper.
	 * @param counts The list of values of the same key of pairs from Mapper.
	 * @param context The bridge streams the reducer output to the framework that will finally write it to HDFS.
	 */
	  private LongWritable total = new LongWritable();

      @Override
      protected void reduce(Text token, Iterable<LongWritable> counts, Context context) throws IOException, InterruptedException {
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