package org.bitmat.querying;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;


import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.bitmat.extras.BitMatRowWritable;
import org.bitmat.extras.StringSerialization;
import org.bitmat.formats.BitMaskOutputFormat;
import org.bitmat.formats.LogicalTableInputFormat;
import org.bitmat.formats.TriplesOutputFormat;
import org.bitmat.formats.WholeFileInputFormat;
import org.bitmat.querying.join.LogicalTableJoinMapper;
import org.bitmat.querying.join.LogicalTableJoinReducer;
import org.bitmat.querying.pruning.BitMatPruningMapper;
import org.bitmat.querying.pruning.BitMatPruningReducer;
import org.bitmat.querying.selection.SelectionMapper;
import org.bitmat.querying.selection.SelectionReducer;
import org.bitmat.utils.Constants;
import org.bitmat.utils.JoinGroup;


import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.core.BasicPattern;


public class MapRedQueryTool extends Configured implements Tool { 

	SPARQLQuery queryRunner = null;
	Query query = null;
	BasicPattern pattern = null;
	List<Node> variableOrder = null;
	HashMap<Node, JoinGroup> nodeToGroup = null;
	HashMap<Integer, JoinGroup> tripleToGroup = null;
	private HashMap<String,Long> idmap = new HashMap<String, Long>();	

	public MapRedQueryTool(SPARQLQuery queryRunner, Query query,String filename_spo) throws ClassNotFoundException, FileNotFoundException, IOException{
		this.queryRunner = queryRunner;
		this.query = query;

		Op queryOp = Algebra.compile(query);
		OpProject project = (OpProject)queryOp;
		OpBGP bgp = (OpBGP)project.getSubOp();
		this.pattern = bgp.getPattern();

		tripleToGroup = new HashMap<Integer, JoinGroup>();

		variableOrder = new ArrayList<Node>();

		nodeToGroup = extractJoinTriples(pattern,tripleToGroup,variableOrder);	

		getListOfWords(this.pattern);

		ObjectInputStream objin = new ObjectInputStream(new FileInputStream(filename_spo));
		HashMap<String, Long> tempmap_spo = (HashMap<String, Long>) objin.readObject();
		objin.close();

		Iterator<Triple> iter = pattern.iterator();
		while(iter.hasNext()){
			Triple triple = iter.next();
			Node object = triple.getObject();
			Node subject = triple.getSubject();
			Node predicate = triple.getPredicate();

			if(object.isConcrete())
				idmap.put(object.toString(), tempmap_spo.get(object.toString()));
			if(subject.isConcrete())
				idmap.put(subject.toString(), tempmap_spo.get(subject.toString()));
			if(predicate.isConcrete())
				idmap.put(predicate.toString(), tempmap_spo.get(predicate.toString()));
		}
	}
	@Override
	public int run(String[] args) throws Exception {
		pruningPhase(args);
		selectionPhase(args);
//		System.exit(0);

		// Next Join phase....
		HashMap<Integer, JoinGroup> joinedGroup = new HashMap<Integer, JoinGroup>();

		Iterator<Node> i  = variableOrder.iterator();
		int joinPhaseCount = 1;
		while(i.hasNext()){

			Node node = i.next();
			JoinGroup group = nodeToGroup.get(node);
			if(group.isJoinable()){
				String joinValue = node.getName();		
				//System.out.println("JoinValue:"+joinValue);

				int noOfTables = group.getSize();
				//System.out.println("NoOfTables:"+noOfTables);

				String tableList = group.getTableList(joinedGroup);
				//System.out.println("TableList:"+tableList);

				List<Path> inputPaths = group.getInputPaths(joinPhaseCount, joinedGroup);

				group.joined(joinPhaseCount+1);
				joinedGroup.putAll(group.getJoinedList());

				joinPhase(joinPhaseCount++, joinValue, noOfTables, tableList, inputPaths, group.getJoinedTableName(""));
			}
		}

		return 0;
	}

	@SuppressWarnings("unused")
	public int joinPhase(int joinPhase,String joinValue, int noOfTables, String tableNames, List<Path> inputPaths, String newTableName) throws IOException{
		System.out.println("Start join phase..");
		JobConf conf = new JobConf(getConf(), getClass());
		conf.setJobName("Map Reduce SPARQL Query Join variable + s");

		System.out.println("conf:"+conf);
		if (conf == null) {
			return -1;
		}		

		conf.set(Constants.JOIN_VALUE, joinValue);
		conf.set(Constants.NO_OF_TABLES, noOfTables+"");
		conf.set(Constants.LIST_OF_TABLES, tableNames);
		conf.set(Constants.NEW_TABLE_NAME, newTableName);

		Iterator<Path> i = inputPaths.iterator();

		while(i.hasNext()){
			Path inputPath = i.next();
			System.out.println("Path in Join:"+ inputPath);
			FileInputFormat.addInputPath(conf, inputPath);
		}
		
		Path outputFilePath = new Path(Constants.OUTPUT_DIR+(joinPhase+1)+"/");
		FileOutputFormat.setOutputPath(conf, outputFilePath);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(LogicalTableJoinMapper.class);
		conf.setReducerClass(LogicalTableJoinReducer.class);

		conf.setInputFormat(LogicalTableInputFormat.class);
		conf.setOutputFormat(TriplesOutputFormat.class);

		FileSystem fs = FileSystem.newInstance(getConf());
		if (fs.exists(outputFilePath)) {
			fs.delete(outputFilePath, true);
		}
		
		JobClient.runJob(conf);

		return 0;
	}

	@SuppressWarnings({ "unused", "unchecked", "rawtypes" })
	public int selectionPhase(String[] args) throws IOException {
		System.out.println("Start selection phase..");
		JobConf conf = new JobConf(getConf(), getClass());

		System.out.println("conf:"+conf);
		if (conf == null) {
			return -1;
		}		
		conf.set(Constants.QUERY_STRING, query.toString());
		conf.set(Constants.ARGS,args.toString());
		conf.set(Constants.IDMAP, StringSerialization.toString(idmap));
		conf.setJobName("Map Reduce SPARQL Query");


		HashMap<String, ArrayList<Integer>> tpmap = new HashMap<String, ArrayList<Integer>>();
		ArrayList<String>  inputPaths = fetchBitMatPathList(tpmap,false);
		Iterator<String> i = inputPaths.iterator();
		while(i.hasNext()){
			String inputPath = i.next();
			FileInputFormat.addInputPath(conf, new Path(inputPath));
		}
		conf.set("tpmap", StringSerialization.toString(tpmap));
		tpmap.clear();
		inputPaths.clear();
		
		Path outputFilePath = new Path("/output1/");
		FileOutputFormat.setOutputPath(conf, outputFilePath);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(SelectionMapper.class);
		conf.setReducerClass(SelectionReducer.class);

		conf.setInputFormat(WholeFileInputFormat.class);
		conf.setOutputFormat(TriplesOutputFormat.class);		


		FileSystem fs = FileSystem.newInstance(getConf());
		if (fs.exists(outputFilePath)) {
			fs.delete(outputFilePath, true);
		}

		System.out.println("Starting JobClient.runJob");
		JobClient.runJob(conf);
		System.out.println("Stop run method..");

		return 0;

	}
	
	@SuppressWarnings("unused")
	public int pruningPhase(String[] args) throws IOException {
		System.out.println("Start pruning phase..");
		JobConf conf = new JobConf(getConf(), getClass());

		System.out.println("conf:"+conf);
		if (conf == null) {
			return -1;
		}		
		conf.set(Constants.QUERY_STRING, query.toString());
		conf.set(Constants.ARGS,args.toString());
		conf.set(Constants.IDMAP, StringSerialization.toString(idmap));
		conf.setJobName("Map Reduce SPARQL Query");

		HashMap<String, ArrayList<Integer>> tpmap = new HashMap<String, ArrayList<Integer>>();
		ArrayList<String>  inputPaths = fetchBitMatPathList(tpmap,true);
		Iterator<String> i = inputPaths.iterator();
		while(i.hasNext()){
			String inputPath = i.next();
			FileInputFormat.addInputPath(conf, new Path(inputPath));
		}
		conf.set("tpmap", StringSerialization.toString(tpmap));
		tpmap.clear();
		inputPaths.clear();
		
		Path outputFilePath = new Path("/bitmasks/");
		FileOutputFormat.setOutputPath(conf, outputFilePath);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(BitMatRowWritable.class);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(BitMatRowWritable.class);

		conf.setMapperClass(BitMatPruningMapper.class);
		conf.setReducerClass(BitMatPruningReducer.class);

		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(BitMaskOutputFormat.class);


		FileSystem fs = FileSystem.newInstance(getConf());
		if (fs.exists(outputFilePath)) {
			fs.delete(outputFilePath, true);
		}

		System.out.println("Starting JobClient.runJob");
		JobClient.runJob(conf);
		System.out.println("Stop run method..");

		return 0;
	}

	public void getListOfWords(BasicPattern pattern){
		System.out.println("Printing Keys Present In The Patterns :");
		Iterator<Triple> i = pattern.getList().iterator();
		while(i.hasNext()){
			Triple triple = i.next();
			String key = null;

			if((key=getKey(triple.getSubject()))!=null) System.out.println(key);
			if((key=getKey(triple.getPredicate()))!=null) System.out.println(key);
			if((key=getKey(triple.getObject()))!=null) System.out.println(key);

		}
	}

	public String getKey(Node node){
		StringBuilder key = new StringBuilder();
		if(node.isConcrete()){
			key.append("[").append(node.toString()).append("]");
			return key.toString();
		}
		return null;
	}

	public ArrayList<String> fetchBitMatPathList(HashMap<String, ArrayList<Integer>> tpmap, boolean needMeta) throws IOException
	{
		Iterator<Triple> iter = pattern.iterator();
		ArrayList<String> inputPaths = new ArrayList<String>();
		int pnum = 0;
		String metaSuffix = needMeta ? ".meta" : "";
		
		while(iter.hasNext()){
			Triple triple = iter.next();
			Node object = triple.getObject();
			Node subject = triple.getSubject();
			Node predicate = triple.getPredicate();

			boolean sub = subject.isConcrete();
			boolean obj = object.isConcrete();
			boolean pred = predicate.isConcrete();

			StringBuilder path = new StringBuilder();
			if (!sub && !obj && !pred) throw new Error("Unhandled case of three variables.");
			else if (!sub && !obj ) {
				path.append(Constants.PREDICATE_SO);
				path.append(Constants.BITMAT);
				path.append(idmap.get(predicate.toString()));
				path.append(metaSuffix);
			}
			else if(!sub && !pred) {
				path.append(Constants.OBJECT);
				path.append(Constants.BITMAT);
				path.append(idmap.get(predicate.toString()));
				path.append(metaSuffix);
			}
			else if(!pred && !obj) {
				path.append(Constants.SUBJECT);
				path.append(Constants.BITMAT);
				path.append(idmap.get(predicate.toString()));
				path.append(metaSuffix);
			}
			else if(!sub) {
				path.append(Constants.PREDICATE_OS);
				path.append(Constants.BITMAT);
				path.append(idmap.get(predicate.toString()));
//				path.append(metaSuffix);
			}
			else if(!obj) {
				path.append(Constants.SUBJECT);
				path.append(Constants.BITMAT);
				path.append(idmap.get(predicate.toString()));
//				path.append(metaSuffix);
			}
			else if(!pred) {
				path.append(Constants.OBJECT);
				path.append(Constants.BITMAT);
				path.append(idmap.get(predicate.toString()));
//				path.append(metaSuffix);
			}
			
			String pstr = path.toString();
			if (!tpmap.containsKey(pstr)) tpmap.put(pstr, new ArrayList());
			tpmap.get(pstr).add(++pnum);
		}
		
		inputPaths.addAll(tpmap.keySet());
		return inputPaths;
	}
	
	public static HashMap<Node, JoinGroup> extractJoinTriples(BasicPattern pattern, HashMap<Integer, JoinGroup> tripleToGroup, List<Node> variableOrder){

		HashMap<Node,JoinGroup> varList = new HashMap<Node, JoinGroup>();		

		Iterator<Triple> i = pattern.iterator();
		int counter = 1;
		while(i.hasNext()){
			Triple triple = i.next();			
			Node object = triple.getObject();
			Node subject = triple.getSubject();
			Node predicate = triple.getPredicate();

			if(subject.isVariable()){
				if(varList.containsKey(subject)) {
					JoinGroup joinGroup = varList.get(subject);
					joinGroup.add(counter,triple);
					tripleToGroup.put(counter, joinGroup);
				}else{
					JoinGroup joinGroup = new JoinGroup();
					joinGroup.add(counter, triple);
					varList.put(subject,joinGroup);
					tripleToGroup.put(counter, joinGroup);
					variableOrder.add(subject);
				}
			}			

			if(predicate.isVariable()){
				if(varList.containsKey(predicate)) {
					JoinGroup joinGroup = varList.get(predicate);
					joinGroup.add(counter,triple);
					tripleToGroup.put(counter, joinGroup);
				}else{
					JoinGroup joinGroup = new JoinGroup();
					joinGroup.add(counter, triple);
					varList.put(predicate,joinGroup);
					tripleToGroup.put(counter, joinGroup);
					variableOrder.add(predicate);
				}
			}

			if(object.isVariable()){
				if(varList.containsKey(object)) {
					JoinGroup joinGroup = varList.get(object);
					joinGroup.add(counter,triple);					
					tripleToGroup.put(counter, joinGroup);
				}else{
					JoinGroup joinGroup = new JoinGroup();
					joinGroup.add(counter, triple);
					varList.put(object,joinGroup);
					tripleToGroup.put(counter, joinGroup);
					variableOrder.add(object);
				}
			}			
			counter++;
		}

		return varList;
	}
}
