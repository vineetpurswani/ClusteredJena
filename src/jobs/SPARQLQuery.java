package jobs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import com.hp.hpl.jena.query.Query;

import arq.query;

public class SPARQLQuery extends query {
	public static final String SPARQL_QUERY_RUNNER = "sparql.query.runner";
	public static final String QUERY_OBJECT = "sparql.query.object";

	public SPARQLQuery(String[] argv) {
		super(argv);		
	}

	public static void main(String[] args){
		SPARQLQuery queryRunner = new SPARQLQuery(Arrays.copyOfRange(args, 1, 2));
		queryRunner.process();
		
		Query query = queryRunner.getQueryObject();
		
		MapRedQueryTool queryTool = null;
		try {
			queryTool = new MapRedQueryTool(queryRunner,query,args[0]);
		} catch (ClassNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		System.out.println("Printing Query Object");
		System.out.println(query);
		System.out.println("Initialized Query tool");
		
		int exitCode = 0;
		try {
			exitCode = ToolRunner.run(new Configuration(),queryTool, args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.exit(exitCode);
	}

	public Query getQueryObject(){
		return modQuery.getQuery();
	}

}
