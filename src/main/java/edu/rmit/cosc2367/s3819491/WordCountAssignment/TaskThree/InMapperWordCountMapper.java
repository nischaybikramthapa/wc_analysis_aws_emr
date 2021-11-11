package edu.rmit.cosc2367.s3819491.WordCountAssignment.TaskThree;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


public class InMapperWordCountMapper extends Mapper<Object, Text, Text, LongWritable> {

	private  Map<String,Integer> map;
	private static final Logger LOG = Logger.getLogger(InMapperWordCountMapper.class);


	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		//initialization
		map = new HashMap<String, Integer>();
	}

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		LOG.setLevel(Level.DEBUG);
		LOG.debug("The mapper task of Nischay Bikram Thapa, s3819491");

		StringTokenizer itr = new StringTokenizer(value.toString());
		
		while (itr.hasMoreTokens()) {
			String token = itr.nextToken();
			Integer count = map.get(token);
			//if count is null initialize to 1
			if(count == null) {
				count = new Integer(1);
			} else {
				//increase count by 1
				count+=1;
			}
			//add token and count in hashmap
			map.put(token,count);

		}
	}	

	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		Iterator<Map.Entry<String, Integer>> itr = map.entrySet().iterator();
		//iterate through hashmap
		while(itr.hasNext()) {
			
			Map.Entry<String, Integer> entry = itr.next();
			String keyVal = entry.getKey()+ "";
			Integer countVal = entry.getValue();
			
			LOG.info("Value inside cleanup is: " + countVal);
			context.write(new Text(keyVal), new LongWritable(countVal));
			
		}
	}
	
}
