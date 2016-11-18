package reducers;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Reporter;

public class SelectionReducer extends MapReduceBase implements
		Reducer<Text, Text, Text, Text> {
	
	@Override
	public void configure(JobConf job) {
	}
	
	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		System.out.println("Reducing..");
		
		StringBuilder builder = new StringBuilder();
		builder.append("Triple_").append(key.toString());
		Text newKey = new Text(builder.toString());
		
		while(values.hasNext()){
			Text value = values.next();
			
			System.out.println("key: "+newKey+", value:"+value);
			output.collect(newKey, value);
		}		
	}

}