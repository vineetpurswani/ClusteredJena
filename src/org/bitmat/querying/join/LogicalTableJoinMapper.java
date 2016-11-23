package org.bitmat.querying.join;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.bitmat.utils.Constants;


import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.core.BasicPattern;



public class LogicalTableJoinMapper extends MapReduceBase
implements Mapper<Text, HashMap<String, String>, Text, Text> {
	
	private String joinValue = null;
	private Query query = null;
	private Op queryOp = null;
	private BasicPattern pattern = null; 
	@Override
	public void configure(JobConf job) {  
		
		this.joinValue = job.get(Constants.JOIN_VALUE);		
		
	}
	
	@Override
	public void close() throws IOException { }

	@Override
	public void map(Text key, HashMap<String,String> value, OutputCollector<Text, Text> output,
			Reporter reporter) throws IOException {		
		Text outputKey = new Text();
		outputKey.set(generateOutputKey(value));		

		Text outputValue = new Text();
		outputValue.set(generateOutputValue(key,value));		
//		System.out.println(outputKey + " " + outputValue);
		output.collect(outputKey, outputValue);
	}
	
	public String generateOutputKey(HashMap<String,String> value){
		StringBuilder tempKey = new StringBuilder();		
		tempKey.append("[").append(joinValue).append("]").append(value.get(joinValue));		
		return tempKey.toString();		
	}
	
	public String generateOutputValue(Text key,HashMap<String,String> value){
		Iterator<Entry<String,String>> i = value.entrySet().iterator();
		StringBuilder builder = new StringBuilder();
		builder.append("{").append(key).append("}");
		while(i.hasNext()){
			Entry<String,String> entry = i.next();
			String varKey = entry.getKey();
			String varValue = entry.getValue();			
			
			if(!joinValue.equals(varKey)){
				builder.append("[").append(varKey).append("]").append(value.get(varKey)).append(Constants.DELIMIT);				
			}			
		}
		
		return builder.toString();
	}
}

