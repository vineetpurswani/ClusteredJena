package org.bitmat.formats;


import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MultiFileInputFormat;
import org.apache.hadoop.mapred.MultiFileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.bitmat.formats.LogicalTableTripleReader;



public class LogicalTableInputFormat extends MultiFileInputFormat<Text, HashMap<String, String>> {

	@Override
	public RecordReader<Text, HashMap<String, String>> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
		return new LogicalTableTripleReader(job,(MultiFileSplit)split);
	}	
	
}