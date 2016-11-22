package org.bitmat.pruning;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.bitmat.extras.StringSerialization;

import utils.Constants;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.core.BasicPattern;

public class BitMatPruningMapper extends MapReduceBase implements Mapper<Text, BytesWritable, Text, BytesWritable>{
	private String queryString = null;
	private Query query = null;
	private Op queryOp = null;
	private BasicPattern pattern = null;
	private String k;
	private Constants.BitMatType bitmat_type = null;
	private int patternNo = 0;
	HashMap<String, ArrayList<Integer>> tpmap = null;
	HashMap<String,Long> idmap = null;
	ArrayList<Integer> patternList = null;

	private void get_pattern()
	{
		//		patternList = tpmap.get(("/"+k.split("/", 4)[3]).replaceFirst(".meta$", ""));
		patternList = tpmap.get(("/"+k.split("/", 4)[3]));

		if(k.contains(Constants.PREDICATE_SO)) bitmat_type = Constants.BitMatType.SO;
		else if(k.contains(Constants.SUBJECT)) bitmat_type = Constants.BitMatType.PO;
		else if(k.contains(Constants.OBJECT)) bitmat_type = Constants.BitMatType.PS;
		else if(k.contains(Constants.PREDICATE_OS)) bitmat_type = Constants.BitMatType.OS;

	}

	@Override
	public void configure(JobConf job) {
		this.queryString = job.get(Constants.QUERY_STRING);

		try {
			this.tpmap = (HashMap<String, ArrayList<Integer>>) StringSerialization.fromString(job.get(Constants.TPMAP));
			this.idmap = (HashMap<String, Long>) StringSerialization.fromString(job.get(Constants.IDMAP));
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		this.query = QueryFactory.create(queryString);

		queryOp = Algebra.compile(query);
		OpProject project = (OpProject)queryOp;
		OpBGP bgp = (OpBGP)project.getSubOp();
		this.pattern = bgp.getPattern();

		this.k = job.get("map.input.file");

		get_pattern();

		// Debugging Section
		System.out.println("map.input.file " + k);
		System.out.println(tpmap);
		
		super.configure(job);
	}

	@Override
	public void map(Text key, BytesWritable value, 
			OutputCollector<Text, BytesWritable> output, Reporter r) throws IOException {

//		System.out.println(key.toString());
//		System.out.println(patternList);
//		System.out.println(bitmat_type);

		for (Integer i : patternList) {
			Triple ttriple = pattern.get(i-1);
			System.out.println(ttriple);
			// Both subject and object are variables
			if (bitmat_type == Constants.BitMatType.SO) {
				if (key.toString().equalsIgnoreCase("Row Vector")) output.collect(new Text(ttriple.getSubject().toString()), value);
				else if (key.toString().equalsIgnoreCase("Column Vector")) output.collect(new Text(ttriple.getObject().toString()), value);
			}
			else if (bitmat_type == Constants.BitMatType.OS) {
				if (key.toString().equalsIgnoreCase("Column Vector")) {
					// System.out.println(ttriple.getSubject().toString() + value.toString());
					output.collect(new Text(ttriple.getSubject().toString()), value);
				}
			}
			else if (bitmat_type == Constants.BitMatType.PS) {
				if (!ttriple.getSubject().isConcrete() && key.toString().equalsIgnoreCase("Column Vector")) output.collect(new Text(ttriple.getSubject().toString()), value);
				else if (key.toString().equalsIgnoreCase("Row Vector")) output.collect(new Text(ttriple.getPredicate().toString()), value);
			}
			else if (bitmat_type == Constants.BitMatType.PO) {
				if (key.toString().equalsIgnoreCase("Column Vector")) output.collect(new Text(ttriple.getObject().toString()), value);
				else if (!ttriple.getPredicate().isConcrete() && key.toString().equalsIgnoreCase("Row Vector")) output.collect(new Text(ttriple.getPredicate().toString()), value);
			}
		}
	}

}
