package format;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.util.Progressable;


public class TriplesOutputFormat extends MultipleTextOutputFormat<Text, Text> {
	
	@Override
	protected RecordWriter<Text, Text> getBaseRecordWriter(FileSystem fs, JobConf job, String name, Progressable progress) throws IOException {
		
		return new TriplesRecordWriter<Text, Text>(job, name, progress);
	}

	@Override
	protected String generateFileNameForKeyValue(Text key, Text value, String name) {		
		return key.toString();
	}
}
