package utils;

public class Constants {
	public static final String MODEL = "model";
	public static final String IDMAP = "key_val";
	public static final String QUERY_STRING = "mapred.sparql.query";
	public static final String ARGS = "mapred.args";
	public static final String JOIN_VALUE = "mapred.join.value";
	public static final String NO_OF_TABLES = "mapred.join.no.of.tables";
	public static final String LIST_OF_TABLES = "mapred.join.list.of.tables";
	public static final String DELIMIT = "||";
	public static final String REGEX = "\\|\\|";
	public static final String OUTPUT_DIR = "/output";
	public static final String BITMAT = "bitmat_";
	public static final String PREDICATE_SO = "/Predicate_so/";
	public static final String PREDICATE_OS = "/Predicate_os/";
	public static final String SUBJECT = "/Subject/";
	public static final String OBJECT = "/Object/";
	public static final String TPMAP = "tpmap";
	public static final String NEW_TABLE_NAME = "NewTableName";
	public static enum BitMatType {
		SO, OS, PS, PO
	}
}