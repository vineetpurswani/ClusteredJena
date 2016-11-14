package mapper;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.bitmat.extras.BitMatRowWritable;

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
	private String conc_key = null;
	private String k;
	private String bitmat_type = null;;
	private int patternNo = 0;
	private HashMap<String,Long> key_val = new HashMap<String, Long>();	

	public Triple get_pattern()
	{
		Iterator<Triple> i = pattern.iterator();
		int pattern_no = 0;
		if(k.contains(Constants.PREDICATE_SO))
		{
			while(i.hasNext()){
				Triple triple = i.next();
				pattern_no++;
				boolean sub = triple.getSubject().isConcrete();
				boolean obj = triple.getObject().isConcrete();
				if(!sub && !obj){
					bitmat_type = "SO";
					patternNo = pattern_no;
					return triple;
				}

			}

		}
		else if(k.contains(Constants.SUBJECT))
		{
			while(i.hasNext()){
				Triple triple = i.next();
				pattern_no++;
				boolean obj = triple.getObject().isConcrete();
				boolean pred = triple.getPredicate().isConcrete();
				if(!pred && !obj){
					bitmat_type = "PO";
					patternNo = pattern_no;
					return triple;
				}
				else if(!obj)
				{
					bitmat_type = "PO";
					patternNo = pattern_no;
					conc_key = key_val.get(triple.getPredicate().toString()).toString();
					return triple;
				}
			}
		}
		else if(k.contains(Constants.OBJECT))
		{
			while(i.hasNext()){
				Triple triple = i.next();			
				boolean sub = triple.getSubject().isConcrete();
				boolean pred = triple.getPredicate().isConcrete();
				pattern_no++;
				if(!sub && !pred){
					bitmat_type = "PS";
					patternNo = pattern_no;
					return triple;
				}
				else if(!pred)
				{
					bitmat_type = "PS";
					patternNo = pattern_no;
					conc_key = key_val.get(triple.getPredicate().toString()).toString();
					return triple;
				}
			}
		}
		else 
		{
			while(i.hasNext()){
				Triple triple = i.next();	
				pattern_no++;
				boolean sub = triple.getSubject().isConcrete();
				if(!sub)
				{
					bitmat_type = "OS";
					patternNo = pattern_no;
					conc_key = key_val.get(triple.getObject().toString()).toString();
					return triple;
				}

			}
		}
		return null;
	}
	@Override
	public void configure(JobConf job) {  

		this.queryString = job.get(Constants.QUERY_STRING);
		String file_name  = job.get(Constants.key_val);
		
		try {
			ObjectInputStream objin = new ObjectInputStream(new FileInputStream(file_name));
			key_val = (HashMap<String, Long>) objin.readObject();
			objin.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		/*String key_pairs = job.get(Constants.key_val);
		
		int d_id = -1,s_id = -1;
		int flag = 0;
		String key = "";
		String Value = "";
		for(int i=0;i<key_pairs.length();i++)
		{
			if(key_pairs.charAt(i) == '$' & (flag == 0))
			{
				d_id = i;
				flag = 1;
				key = key_pairs.substring(s_id+1, d_id);
			}
			else if((key_pairs.charAt(i) == ' ') & (flag == 1))
			{
				Value = key_pairs.substring(d_id+1, i);
				s_id = i;
				flag = 0;
				key_val.put(key, Long.parseLong(Value));
			}
		}
		*/ catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		//		ModQueryIn queryMod = new ModQueryIn();
		//		queryMod.queryString = queryString;
		this.query = QueryFactory.create(queryString);
		System.out.println("Query object in Configure ");
		System.out.println(this.query);


		queryOp = Algebra.compile(query);

		OpProject project = (OpProject)queryOp;
		OpBGP bgp = (OpBGP)project.getSubOp();
		pattern = bgp.getPattern();
		k = job.get("map.input.file");
		
		System.out.println("map.input.file  " + k);
	}

	@Override
	public void close() throws IOException { }

	public Text get_value(Long row_id,Long col_id,Triple to_match)
	{
		StringBuilder value = new StringBuilder();
		if(bitmat_type == "SO"){
			value.append("[").append(to_match.getSubject().getName()).append("]").append(""+row_id).append(Constants.DELIMIT);
			value.append("[").append(to_match.getObject().getName()).append("]").append(""+col_id).append(Constants.DELIMIT);
		}
		else if(bitmat_type == "OS"){
			value.append("[").append(to_match.getObject().getName()).append("]").append(""+row_id).append(Constants.DELIMIT);
			value.append("[").append(to_match.getSubject().getName()).append("]").append(""+col_id).append(Constants.DELIMIT);
		}
		else if(bitmat_type == "PO"){
			value.append("[").append(to_match.getPredicate().getName()).append("]").append(""+row_id).append(Constants.DELIMIT);
			value.append("[").append(to_match.getObject().getName()).append("]").append(""+col_id).append(Constants.DELIMIT);
		}
		else
		{
			value.append("[").append(to_match.getPredicate().getName()).append("]").append(""+row_id).append(Constants.DELIMIT);
			value.append("[").append(to_match.getSubject().getName()).append("]").append(""+col_id).append(Constants.DELIMIT);
		}
		return new Text(value.toString());
	}


	@Override
	public void map(Text key, BitMatWritable value,
			OutputCollector<Text, Text> output, Reporter arg3) throws IOException {

		
		System.out.println(value.rows.size());
		
		Triple to_match = get_pattern();

		if(conc_key == null)					// Only one concrete value in the Triple
		{
			for(int i=0;i<value.rows.size();i++)
			{

				BitMatRowWritable r = value.rows.get(i);
				boolean cur_bit = r.firstBit;
				Long start_col_id = 0L;
				Long start_row_id = r.rowId;
				System.out.println("Size " + r.rowRest.size());
				for(int j=0;j<r.rowRest.size();j++)
				{
					System.out.println("First_Bit " + cur_bit + " Row_value" + r.rowRest.get(j));
					if(!cur_bit)
						start_col_id += r.rowRest.get(j);
					else
					{
						Long col_val = start_col_id + r.rowRest.get(j);
						for(;start_col_id < col_val;start_col_id++)
						{
							System.out.println(start_row_id.toString() + " " + start_col_id.toString());
							output.collect(new Text(""+patternNo),get_value(start_row_id,start_col_id,to_match));
						}
					}
					cur_bit = !cur_bit;
				}

			}
		}

		else 
		{
			Long key_row_id = Long.parseLong(conc_key);
			boolean flag = true;
			for(int i=0;i<value.rows.size() && flag;i++)
			{

				BitMatRowWritable r = value.rows.get(i);
				boolean cur_bit = r.firstBit;
				Long start_col_id = 0L;
				Long start_row_id = r.rowId;
				if(key_row_id == start_row_id)
				{
					for(int j=0;j<r.rowRest.size();j++)
					{
						if(!cur_bit)
							start_col_id += r.rowRest.get(j);
						else
						{
							Long col_val = start_col_id + r.rowRest.get(j);
							for(;start_col_id < col_val;start_col_id++)
							{
								output.collect(new Text(""+patternNo),get_value(start_row_id,start_col_id,to_match));
							}
						}
						cur_bit = !cur_bit;
					}
					flag = false;
				}


			}

		}
	}


}
