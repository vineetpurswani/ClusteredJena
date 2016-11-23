package org.bitmat.querying.pruning;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.bitmat.extras.BitMatRowWritable;
import org.bitmat.extras.StringSerialization;
import org.bitmat.utils.Constants;


import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.core.BasicPattern;

public class BitMatPruningReducer extends MapReduceBase implements Reducer<Text, BitMatRowWritable, Text, BitMatRowWritable> {
	private BasicPattern pattern = null;
	
	private ArrayList<Integer> tpnodeList(String tvar) {
		ArrayList<Integer> tpnodes = new ArrayList<Integer>();
		for (int i=0; i<pattern.size(); i++) {
			Triple ttriple = pattern.get(i);
			if (ttriple.getSubject().toString().equals(tvar) ||
					ttriple.getPredicate().toString().equals(tvar) ||
					ttriple.getObject().toString().equals(tvar)) 
				tpnodes.add(i+1);
		}
		return tpnodes;
	}
	
	public void configure(JobConf job) {
		String queryString = job.get(Constants.QUERY_STRING);
		Query query = QueryFactory.create(queryString);
		Op queryOp = Algebra.compile(query);
		OpProject project = (OpProject)queryOp;
		OpBGP bgp = (OpBGP)project.getSubOp();
		this.pattern = bgp.getPattern();
		super.configure(job);
	}
	
	@Override
	public void reduce(Text key, Iterator<BitMatRowWritable> value,
			OutputCollector<Text, BitMatRowWritable> output, Reporter r)
			throws IOException {
		BitMatRowWritable row = new BitMatRowWritable(-1L);
		System.out.println(key);
		while (value.hasNext()) {
			BitMatRowWritable t = value.next(); 
			System.out.println(t);
			row.unFold(t);
		}
		System.out.println(row);
		
		for (Integer tid : tpnodeList(key.toString())) {
			output.collect(new Text("tpbm_"+tid+"["+key.toString()+"]"), row);
		}
	}

}
