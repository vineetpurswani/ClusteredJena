package org.bitmat.formats;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleSequenceFileOutputFormat;
import org.bitmat.extras.BitMatRowWritable;

public class BitMaskOutputFormat extends MultipleSequenceFileOutputFormat<Text, BitMatRowWritable> {

	@Override
	protected String generateFileNameForKeyValue(Text key, BitMatRowWritable value,
			String name) {
//		return super.generateFileNameForKeyValue(key, value, name);
		return key.toString();
	}

}
