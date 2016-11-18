package mapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.bitmat.extras.BitMatRowWritable;
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

import format.BitMatWritable;

public class SelectionMapper extends MapReduceBase implements Mapper<Text, BitMatWritable, Text, Text>{

	private String queryString = null;
	private Query query = null;
	private Op queryOp = null;
	private BasicPattern pattern = null;
	private String k;
	private String bitmat_type = null;;
	private int patternNo = 0;
	HashMap<String, ArrayList<Integer>> tpmap = null;
	HashMap<String,Long> idmap = null;

	public ArrayList<Integer> get_pattern()
	{
		ArrayList<Integer> patternList = tpmap.get("/"+k.split("/", 4)[3]);

		if(k.contains(Constants.PREDICATE_SO)) bitmat_type = "SO";
		else if(k.contains(Constants.SUBJECT)) bitmat_type = "PO";
		else if(k.contains(Constants.OBJECT)) bitmat_type = "PS";
		else bitmat_type = "OS";

		return patternList;
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

		// Debugging Section
		System.out.println("map.input.file" + k);
	}

	@Override
	public void close() throws IOException { }

	public Text get_value(Long row_id,Long col_id,Triple to_match, boolean isTwoVar)
	{
		StringBuilder value = new StringBuilder();
		if(bitmat_type == "SO"){
			value.append("[").append(to_match.getSubject().getName()).append("]").append(""+row_id).append(Constants.DELIMIT);
			value.append("[").append(to_match.getObject().getName()).append("]").append(""+col_id).append(Constants.DELIMIT);
		}
		else if(bitmat_type == "OS"){
			if (isTwoVar) value.append("[").append(to_match.getObject().getName()).append("]").append(""+row_id).append(Constants.DELIMIT);
			value.append("[").append(to_match.getSubject().getName()).append("]").append(""+col_id).append(Constants.DELIMIT);
		}
		else if(bitmat_type == "PO"){
			if (isTwoVar) value.append("[").append(to_match.getPredicate().getName()).append("]").append(""+row_id).append(Constants.DELIMIT);
			value.append("[").append(to_match.getObject().getName()).append("]").append(""+col_id).append(Constants.DELIMIT);
		}
		else
		{
			if (isTwoVar) value.append("[").append(to_match.getPredicate().getName()).append("]").append(""+row_id).append(Constants.DELIMIT);
			value.append("[").append(to_match.getSubject().getName()).append("]").append(""+col_id).append(Constants.DELIMIT);
		}
		return new Text(value.toString());
	}

	public void matchTriples(BitMatWritable bitmat, ArrayList<Integer> patternList, OutputCollector<Text, Text> output) throws IOException {
		HashMap<Long, ArrayList<Integer>> concreteNodes = new HashMap<Long, ArrayList<Integer>>();
		ArrayList<Integer> tpTwoVariable = new ArrayList<Integer>();
		for (Integer i : patternList) {
			Triple ttriple = pattern.get(i-1);
			// Case 1 : Two Variables
			if (ttriple.getObject().isConcrete() ^ ttriple.getSubject().isConcrete() ^ ttriple.getPredicate().isConcrete() == true) {
				tpTwoVariable.add(i);
			}
			// Case 2 : Single Variable
			else {
				System.out.println(ttriple);
				if (bitmat_type == "SO"); 									// not possible
				else if (bitmat_type == "OS") { 							// object is concrete
					System.out.println(ttriple.getObject().toString());
					System.out.println(idmap);
					Long obj = idmap.get(ttriple.getObject().toString());
					if (!concreteNodes.containsKey(obj)) concreteNodes.put(obj, new ArrayList<Integer>());
					concreteNodes.get(obj).add(i);
				}
				else if (bitmat_type == "PO" || bitmat_type == "PS") { 		// predicate might be concrete
					if (!ttriple.getPredicate().isConcrete()) continue;
					Long pred = idmap.get(ttriple.getPredicate().toString());
					if (!concreteNodes.containsKey(pred)) concreteNodes.put(pred, new ArrayList<Integer>());
					concreteNodes.get(pred).add(i);
				}
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
						for (Integer twoVar : tpTwoVariable)
							output.collect(new Text(twoVar.toString()),get_value(start_row_id, start_col_id, pattern.get(twoVar - 1), true));
						if (concreteNodes.containsKey(start_row_id)) {
							for (Integer oneVar : concreteNodes.get(start_row_id))
								output.collect(new Text(oneVar.toString()),get_value(start_row_id, start_col_id, pattern.get(oneVar - 1), false));
						}
					}
				}
				cur_bit = !cur_bit;
			}
		}
	}

	@Override
	public void map(Text key, BitMatWritable value,
			OutputCollector<Text, Text> output, Reporter r) throws IOException {

		System.out.println(value.rows.size());
		System.out.println(patternNo);

		ArrayList<Integer> patternList = get_pattern();
		matchTriples(value, patternList, output);

		//		if(conc_key == null)					// Only one concrete value in the Triple
		//		{
		//			for(int i=0;i<value.rows.size();i++)
		//			{
		//				BitMatRowWritable r = value.rows.get(i);
		//				boolean cur_bit = r.firstBit;
		//				Long start_col_id = 0L;
		//				Long start_row_id = r.rowId;
		//				for(int j=0;j<r.rowRest.size();j++)
		//				{
		//					if(!cur_bit)
		//						start_col_id += r.rowRest.get(j);
		//					else
		//					{
		//						Long col_val = start_col_id + r.rowRest.get(j);
		//						for(;start_col_id < col_val;start_col_id++)
		//						{
		//							output.collect(new Text(""+patternNo),get_value(start_row_id,start_col_id,to_match));
		//						}
		//					}
		//					cur_bit = !cur_bit;
		//				}
		//
		//			}
		//		}
		//
		//		else 
		//		{
		//			Long key_row_id = Long.parseLong(conc_key);
		//			boolean flag = true;
		//			for(int i=0;i<value.rows.size() && flag;i++)
		//			{
		//				BitMatRowWritable r = value.rows.get(i);
		//				boolean cur_bit = r.firstBit;
		//				Long start_col_id = 0L;
		//				Long start_row_id = r.rowId;
		//				if(key_row_id == start_row_id)
		//				{
		//					for(int j=0;j<r.rowRest.size();j++)
		//					{
		//						if(!cur_bit)
		//							start_col_id += r.rowRest.get(j);
		//						else
		//						{
		//							Long col_val = start_col_id + r.rowRest.get(j);
		//							for(;start_col_id < col_val;start_col_id++)
		//							{
		//								output.collect(new Text(""+patternNo),get_value(start_row_id,start_col_id,to_match));
		//							}
		//						}
		//						cur_bit = !cur_bit;
		//					}
		//					flag = false;
		//				}
		//
		//
		//			}
		//
		//		}
	}


}
