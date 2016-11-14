package format; 
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.bitmat.extras.BitMatRowWritable;

public class WholeFileRecordReader implements RecordReader<Text, BitMatWritable> {
	private FileSplit fileSplit;
	private Configuration conf;
	private BitMatWritable value = new BitMatWritable();
	private boolean processed = false;

	public WholeFileRecordReader(FileSplit fileSplit, Configuration conf) throws IOException {
		this.fileSplit = fileSplit;
		this.conf = conf;
	}

	@Override
	public void close() throws IOException {
	}

	@Override
	public Text createKey() {
		return new Text();
	}
	@Override
	public BitMatWritable createValue() {
		return new BitMatWritable();
	}
	@Override
	public long getPos() throws IOException {
		// TODO Auto-generated method stub
		return processed ? fileSplit.getLength() : 0;
	}
	@Override
	public float getProgress() throws IOException {
		// TODO Auto-generated method stub
		return processed ? 1.0f : 0.0f;
	}
	@Override
	public boolean next(Text key, BitMatWritable value) throws IOException {
		if (!processed) {
			Path file = fileSplit.getPath();
			key.set(file.getName());
			FileSystem fs = file.getFileSystem(conf);
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, file, conf);
			try {
				WritableComparable nul = (WritableComparable) reader.getKeyClass().newInstance();
				Writable val = (Writable) reader.getValueClass().newInstance();

				while (reader.next(nul,val)) {
					value.addRow((BitMatRowWritable) val);  
				}

				value.setId(((LongWritable)nul).get());
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			reader.close();

			processed = true;
			return true;
		}
		return false;
	}

	//	  @Override
	//	  public void initialize(InputSplit split, TaskAttemptContext context)
	//	      throws IOException, InterruptedException {
	//	    this.fileSplit = (FileSplit) split;
	//	    this.conf = context.getConfiguration();
	//	  }

	//	@Override
	//	public boolean nextKeyValue() throws IOException, InterruptedException {
	//		if (!processed) {
	//			//	      byte[] contents = new byte[(int) fileSplit.getLength()];
	//			Path file = fileSplit.getPath();
	//			FileSystem fs = file.getFileSystem(conf);
	//			//FSDataInputStream in = null;
	//
	//			SequenceFile.Reader reader = new SequenceFile.Reader(fs, file, conf);
	//
	//
	//			try {
	//				WritableComparable nul = (WritableComparable) reader.getKeyClass().newInstance();
	//				Writable val = (Writable) reader.getValueClass().newInstance();
	//
	//				while (reader.next(nul,val)) {
	//					value.addRow((BitMatRowWritable) val);  
	//				}
	//
	//				value.setId(((LongWritable)nul).get());
	//			} catch (InstantiationException e) {
	//				// TODO Auto-generated catch block
	//				e.printStackTrace();
	//			} catch (IllegalAccessException e) {
	//				// TODO Auto-generated catch block
	//				e.printStackTrace();
	//			}
	//
	//			reader.close();
	//
	//			processed = true;
	//			return true;
	//		}
	//		return false;
	//	}
	//
	//	@Override
	//	public Text getCurrentKey() throws IOException, InterruptedException {
	//		return new Text(fileSplit.getPath().toString());
	//	}
	//
	//	@Override
	//	public BitMatWritable getCurrentValue() throws IOException,
	//	InterruptedException {
	//		return value;
	//	}

}
