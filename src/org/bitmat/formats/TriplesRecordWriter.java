package org.bitmat.formats;

import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

public class TriplesRecordWriter<K, V> implements RecordWriter<K, V> {
	 private OutputStreamWriter out = null;
	 //private FSDataOutputStream out = null;
	 private Path file = null;
	 
	 public TriplesRecordWriter(JobConf job, String name, Progressable progress) {
		 try {
			file = new Path(FileOutputFormat.getOutputPath(job), name);
		    FileSystem fs = file.getFileSystem(job);
		    out = new OutputStreamWriter(fs.create(file, progress));
		} catch (Exception e) {			
			System.out.println("Couldn't create file");
		}
	 }

	@Override
	public void close(Reporter reporter) throws IOException {
		out.close();
	}

	@Override
	public void write(K key, V value) throws IOException {
		out.write(value.toString()+"\n");
	}
}
