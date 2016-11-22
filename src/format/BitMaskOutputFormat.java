package format;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleSequenceFileOutputFormat;

public class BitMaskOutputFormat extends MultipleSequenceFileOutputFormat<Text, BytesWritable> {

	@Override
	protected String generateFileNameForKeyValue(Text key, BytesWritable value,
			String name) {
//		return super.generateFileNameForKeyValue(key, value, name);
		return key.toString();
	}

}
