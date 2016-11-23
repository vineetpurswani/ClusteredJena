package org.bitmat.formats;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;
import org.bitmat.extras.BitMatRowWritable;


public class BitMatWritable implements Writable {
	public ArrayList<BitMatRowWritable> rows;
	public Long matId;
	
	BitMatWritable() {
		rows = new ArrayList<BitMatRowWritable>();
		matId = -1L;
	}
	
	public void setId(Long id) {
		matId = id;
	}
	
	public void addRow(BitMatRowWritable row) {
		rows.add(new BitMatRowWritable(row));
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		String res = matId.toString()+"\n";
		for (BitMatRowWritable i : rows) {
			res += i.toString()+"\n";
		}
		return res;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		
	}

}

