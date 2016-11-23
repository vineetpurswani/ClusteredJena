package org.bitmat.querying.join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Reporter;
import org.bitmat.utils.Constants;


public class LogicalTableJoinReducer extends MapReduceBase implements
Reducer<Text, Text, Text, Text> {

	ArrayList<String> tables;
	int noOfTables = 0;
	String newTableName = "";

	public boolean equalLists(List<String> one, List<String> two){
	    if (one == null && two == null){
	        return true;
	    }

	    if((one == null && two != null) 
	      || one != null && two == null
	      || one.size() != two.size()){
	        return false;
	    }

	    return one.equals(two);
	}
	
	@Override
	public void configure(JobConf job) {
		String noOfTables = job.get(Constants.NO_OF_TABLES);
		String listOfTables = job.get(Constants.LIST_OF_TABLES);
		newTableName = job.get(Constants.NEW_TABLE_NAME);

		if(listOfTables!=null){
			tables = new ArrayList<String>(Arrays.asList(listOfTables.split(Constants.REGEX)));			
		}
		Collections.sort(tables);
	}

	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
					throws IOException {
		HashSet<String> files = new HashSet<String>();
		HashMap<String, ArrayList<String>> remainingStringList = new HashMap<String, ArrayList<String>>();

		while(values.hasNext()){
			String value = values.next().toString();			
			if(value.length()>0){			
				int fileNameEnds = value.lastIndexOf('}');
				String tableName = value.substring(1,fileNameEnds);
				files.add(tableName);

				String restOfString = value.substring(fileNameEnds+1,value.length());
				if (!remainingStringList.containsKey(tableName)) remainingStringList.put(tableName, new ArrayList<String>());
				if(restOfString!=null && restOfString.length()>1){
					remainingStringList.get(tableName).add(restOfString);
				}
			}
		}	

		String newTableName = null;
		if(validGroup(files)){			        	
			emitResults(remainingStringList, new ArrayList<String>(remainingStringList.keySet()), 0, key+Constants.DELIMIT, output);
		}
	}

	private void emitResults(HashMap<String, ArrayList<String>> remainingStringList,
			ArrayList<String> keyList, int index, String newValue,
			OutputCollector<Text, Text> output) throws IOException {
		if (index == keyList.size()) {
//			System.out.println(newTableName + " " + newValue.toString());
			output.collect(new Text(newTableName), new Text(newValue));
			return;
		}
		
		ArrayList<String> stringList = remainingStringList.get(keyList.get(index));
		for (String s : stringList) {
			emitResults(remainingStringList, keyList, index+1, newValue+s, output);
		}
		if (stringList.size() == 0) 
			emitResults(remainingStringList, keyList, index+1, newValue, output);
	}

	private boolean validGroup(HashSet<String> files){
		ArrayList<String> tfiles= new ArrayList<String>(files);   
	    Collections.sort(tfiles);
	    
		if (!equalLists(tables, tfiles)) 
			return false;
		return true;
	}

}
