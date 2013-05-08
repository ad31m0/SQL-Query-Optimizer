package query;

import heap.HeapFile;
import index.HashIndex;
import global.Minibase;
import global.RID;
import global.SearchKey;
import parser.AST_Insert;
import primitives.Schema;
import primitives.Tuple;

/**
 * Execution plan for inserting tuples.
 */
class Insert implements Plan {

	private String fileName;
	private Schema schema;
	private Tuple tuple;
	

	/**
	 * Optimizes the plan, given the parsed query.
	 * 
	 * @throws QueryException
	 *             if table doesn't exists or values are invalid
	 */
	public Insert(AST_Insert tree) throws QueryException {
		fileName = tree.getFileName();
		QueryCheck.tableExists(fileName);
		schema = Minibase.SystemCatalog.getSchema(fileName);
		QueryCheck.insertValues(schema, tree.getValues());
		tuple = new Tuple(schema , tree.getValues());
		//TODO check for size and types
	} // public Insert(AST_Insert tree) throws QueryException

	/**
	 * Executes the plan and prints applicable output.
	 */
	public void execute() {
		RID rid = new HeapFile(fileName).insertRecord(tuple.getData());
		int[] allcols = new int[schema.getCount()];
		for(int i=0; i<schema.getCount(); i++)
			allcols[i] = i;
		IndexDesc[] indexes = Minibase.SystemCatalog.getIndexes(fileName, schema, allcols);
		for(IndexDesc index : indexes)
			new HashIndex(index.indexName).insertEntry(new SearchKey(tuple.getField(index.columnName)), rid);
		
		// print the output message
		System.out.println("1 rows inserted.");

	} // public void execute()

} // class Insert implements Plan
