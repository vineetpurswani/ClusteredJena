package org.bitmat.querying.selection;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.bitmat.extras.BitMatRowWritable;
import org.bitmat.extras.StringSerialization;
import org.bitmat.formats.BitMatWritable;
import org.bitmat.utils.Constants;


import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.core.BasicPattern;


public class SelectionMapper extends MapReduceBase implements Mapper<Text, BitMatWritable, Text, Text>{

	private String queryString = null;
	private Query query = null;
	private Op queryOp = null;
	private BasicPattern pattern = null;
	private String k;
	private Constants.BitMatType bitmat_type = null;
	private int patternNo = 0;
	private HashMap<String, ArrayList<Integer>> tpmap = null;
	private HashMap<String,Long> idmap = null;
//	private HashMap<String,ArrayList<Boolean>> tpBitmaskMap = new HashMap<String,ArrayList<Boolean>>();
	private HashMap<String,boolean[]> tpBitmaskMap = new HashMap<String, boolean[]>();
	private JobConf conf = null;

	private void readBitMasks(String key) {
		try {
			Path file = new Path("/bitmasks/"+key);
			FileSystem fs = file.getFileSystem(conf);
			SequenceFile.Reader min = new SequenceFile.Reader(fs, file, conf);
			Writable nkey = (Writable) min.getKeyClass().newInstance();
			Writable val = (Writable) min.getValueClass().newInstance();
			min.next(nkey, val);
			min.close();

			tpBitmaskMap.putIfAbsent(key, ((BitMatRowWritable)val).toBoolArray());
			System.out.println((BitMatRowWritable)val);
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		catch (InstantiationException e) {
			e.printStackTrace();
		}
		catch (IllegalAccessException e) {
			e.printStackTrace();
		}
	}

	private ArrayList<Integer> get_pattern() {
		ArrayList<Integer> patternList = tpmap.get("/"+k.split("/", 4)[3]);

		if(k.contains(Constants.PREDICATE_SO)) bitmat_type = Constants.BitMatType.SO;
		else if(k.contains(Constants.SUBJECT)) bitmat_type = Constants.BitMatType.PO;
		else if(k.contains(Constants.OBJECT)) bitmat_type = Constants.BitMatType.PS;
		else if(k.contains(Constants.PREDICATE_OS)) bitmat_type = Constants.BitMatType.OS;
		else throw new Error("Invalid file encountered.");

		for (Integer i : patternList) {
			Triple ttriple = pattern.get(i-1);
			if (!ttriple.getSubject().isConcrete())
				readBitMasks("tpbm_"+i+"["+ttriple.getSubject().toString()+"]");
			if (!ttriple.getObject().isConcrete()) 
				readBitMasks("tpbm_"+i+"["+ttriple.getObject().toString()+"]");
			if (!ttriple.getPredicate().isConcrete()) 
				readBitMasks("tpbm_"+i+"["+ttriple.getPredicate().toString()+"]");
		}

		System.out.println(tpBitmaskMap);
		return patternList;
	}

	private String get_value(Long row_id, Long col_id, Integer tindex, boolean isTwoVar) {
		try {
			Triple to_match = pattern.get(tindex-1);
			StringBuilder value = new StringBuilder();
			if(bitmat_type == Constants.BitMatType.SO){
				if (!tpBitmaskMap.get("tpbm_"+tindex+"["+to_match.getSubject().toString()+"]")[row_id.intValue()]) return null;
				if (!tpBitmaskMap.get("tpbm_"+tindex+"["+to_match.getObject().toString()+"]")[col_id.intValue()]) return null;
				value.append("[").append(to_match.getSubject().getName()).append("]").append(""+row_id).append(Constants.DELIMIT);
				value.append("[").append(to_match.getObject().getName()).append("]").append(""+col_id).append(Constants.DELIMIT);
			}
			else if(bitmat_type == Constants.BitMatType.OS){
				if (!tpBitmaskMap.get("tpbm_"+tindex+"["+to_match.getSubject().toString()+"]")[col_id.intValue()]) return null;
				//			if (isTwoVar && !tpBitmaskMap.get("tpbm_"+tindex+"["+to_match.getObject().toString()+"]").get(row_id.intValue())) return null;
				//			if (isTwoVar) value.append("[").append(to_match.getObject().getName()).append("]").append(""+row_id).append(Constants.DELIMIT);
				value.append("[").append(to_match.getSubject().getName()).append("]").append(""+col_id).append(Constants.DELIMIT);
			}
			else if(bitmat_type == Constants.BitMatType.PO){
				if (isTwoVar && !tpBitmaskMap.get("tpbm_"+tindex+"["+to_match.getPredicate().toString()+"]")[row_id.intValue()]) return null;
				if (!tpBitmaskMap.get("tpbm_"+tindex+"["+to_match.getObject().toString()+"]")[col_id.intValue()]) return null;
				if (isTwoVar) value.append("[").append(to_match.getPredicate().getName()).append("]").append(""+row_id).append(Constants.DELIMIT);
				value.append("[").append(to_match.getObject().getName()).append("]").append(""+col_id).append(Constants.DELIMIT);
			}
			else if(bitmat_type == Constants.BitMatType.PS){
				if (isTwoVar && !tpBitmaskMap.get("tpbm_"+tindex+"["+to_match.getSubject().toString()+"]")[col_id.intValue()]) return null;
				if (!tpBitmaskMap.get("tpbm_"+tindex+"["+to_match.getPredicate().toString()+"]")[row_id.intValue()]) return null;
				value.append("[").append(to_match.getPredicate().getName()).append("]").append(""+row_id).append(Constants.DELIMIT);
				if (isTwoVar) value.append("[").append(to_match.getSubject().getName()).append("]").append(""+col_id).append(Constants.DELIMIT);
			}
			return value.toString();
		}
		catch (ArrayIndexOutOfBoundsException e) {
			e.printStackTrace();
			return null;
		}
	}

	private void matchTriples(BitMatWritable bitmat, ArrayList<Integer> patternList, 
			OutputCollector<Text, Text> output) throws IOException {
		HashMap<Long, ArrayList<Integer>> concreteNodesRow = new HashMap<Long, ArrayList<Integer>>();
		HashMap<Long, ArrayList<Integer>> concreteNodesColumn = new HashMap<Long, ArrayList<Integer>>();
		ArrayList<Integer> tpTwoVariable = new ArrayList<Integer>();
		String value;
		for (Integer i : patternList) {
			Triple ttriple = pattern.get(i-1);
			// Case 1 : Two Variables
			if (ttriple.getObject().isConcrete() ^ ttriple.getSubject().isConcrete() ^ ttriple.getPredicate().isConcrete() == true) {
				tpTwoVariable.add(i);
			}
			// Case 2 : Single Variable
			else {
				System.out.println(ttriple);
				if (bitmat_type == Constants.BitMatType.SO); 								// not possible
				else if (bitmat_type == Constants.BitMatType.OS) { 							// object is concrete
					Long obj = idmap.get(ttriple.getObject().toString());
					if (!concreteNodesRow.containsKey(obj)) concreteNodesRow.put(obj, new ArrayList<Integer>());
					concreteNodesRow.get(obj).add(i);
				}
				else if (bitmat_type == Constants.BitMatType.PO) { 							// predicate might be concrete
					if (!ttriple.getPredicate().isConcrete()) continue;
					Long pred = idmap.get(ttriple.getPredicate().toString());
					if (!concreteNodesRow.containsKey(pred)) concreteNodesRow.put(pred, new ArrayList<Integer>());
					concreteNodesRow.get(pred).add(i);
				}
				else if (bitmat_type == Constants.BitMatType.PS) {							// subject might be concrete
					if (!ttriple.getSubject().isConcrete()) continue;
					Long sub = idmap.get(ttriple.getSubject().toString());
					if (!concreteNodesColumn.containsKey(sub)) concreteNodesColumn.put(sub, new ArrayList<Integer>());
					concreteNodesColumn.get(sub).add(i);
				}
				// TODO SPO all variable
			}
		}

		for(int i=0;i<bitmat.rows.size();i++)
		{
			BitMatRowWritable r = bitmat.rows.get(i);
			boolean cur_bit = r.firstBit;
			Long start_col_id = 0L;
			Long start_row_id = r.rowId;
			for(int j=0;j<r.rowRest.size();j++)
			{
				if(!cur_bit)
					start_col_id += r.rowRest.get(j);
				else
				{
					Long col_val = start_col_id + r.rowRest.get(j);
					for(;start_col_id < col_val;start_col_id++)
					{
						for (Integer twoVar : tpTwoVariable) {
							if ((value = get_value(start_row_id, start_col_id, twoVar, true)) != null) {
								output.collect(new Text(twoVar.toString()), new Text(value));
							}
						}
						if (concreteNodesRow.containsKey(start_row_id)) {
							for (Integer oneVar : concreteNodesRow.get(start_row_id)) {
								if ((value = get_value(start_row_id, start_col_id, oneVar, false)) != null) {
									output.collect(new Text(oneVar.toString()), new Text(value));
								}
							}
						}
						if (concreteNodesColumn.containsKey(start_col_id)) {
							for (Integer oneVar : concreteNodesColumn.get(start_col_id)) {
								if ((value = get_value(start_row_id, start_col_id, oneVar, false)) != null) {
									output.collect(new Text(oneVar.toString()), new Text(value));
								}
							}
						}
					}
				}
				cur_bit = !cur_bit;
			}
		}
	}

	@Override
	public void configure(JobConf job) {  
		this.queryString = job.get(Constants.QUERY_STRING);
		this.conf = job;

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

		// Debugging Section
		System.out.println("map.input.file" + k);
	}

	@Override
	public void close() throws IOException { }

	@Override
	public void map(Text key, BitMatWritable value,
			OutputCollector<Text, Text> output, Reporter r) throws IOException {

//		System.out.println(value.rows.size());
//		System.out.println(patternNo);

		ArrayList<Integer> patternList = get_pattern();
		matchTriples(value, patternList, output);
	}


}
