package query;

import global.Minibase;
import heap.HeapFile;
import iterators.Iterator;
import iterators.relop.CNFSelection;
import iterators.relop.Projection;
import iterators.relop.SimpleJoin;
import iterators.scanners.FileScan;
import parser.AST_Select;
import primitives.Predicate;
import primitives.Schema;

/**
 * Execution plan for selecting tuples.
 */
class Select implements Plan
{

	private String[] tables;
	private String[] columns;
	private Predicate[][] predicates;

	/**
	 * Optimizes the plan, given the parsed query.
	 * 
	 * @throws QueryException
	 *             if validation fails
	 */
	public Select(AST_Select tree) throws QueryException
	{
		
		this.tables = tree.getTables();
		this.columns = tree.getColumns();
		this.predicates = tree.getPredicates();
		
		validateQuery();
	} // public Select(AST_Select tree) throws QueryException
	
	
	private void validateQuery() throws QueryException
	{
		// Check tables existence in the database
		
				Schema initial_schema1 = QueryCheck.tableExists(tables[0]);
				Schema joined_schema ;
				if(tables.length>1)
				{
					Schema initial_schema2 = QueryCheck.tableExists(tables[1]);
					joined_schema = Schema.join(initial_schema1, initial_schema2);
					for(int i = 2 ; i < tables.length ; i++)
					{
						Schema next_schema = Schema.join(joined_schema, QueryCheck.tableExists(tables[i]));
						joined_schema = next_schema ;
					}
				}
				else
				{
					joined_schema = initial_schema1;
				}
				
				// Check columns existence in the joined schema
						
				for(int i = 0 ; i < columns.length ; i++)
				{
					QueryCheck.columnExists(joined_schema, columns[i]);
				}
				
				// Check predicates validity on the joined schema
				
				for(int i = 0 ; i < predicates.length ; i++)
				{
					for (int j = 0; j < predicates[0].length; j++)
					{
						if (!predicates[i][j].validate(joined_schema))
						{
							throw new QueryException("Invalid Predicate : @ " + predicates[i][j].toString());
						}
					}
				}
	}

	/**
	 * Executes the plan and prints applicable output.
	 */
	public void execute()
	{

		// print the output message
		int cnt = executeBasicPlan();
		System.out.println(cnt + " rows selected");

	} // public void execute()

	
	private int executeBasicPlan()
	{
		// Get joined schema
		
		Schema initialSchema1 = Minibase.SystemCatalog.getSchema(tables[0]);
		HeapFile initialFile1 = new HeapFile(tables[0]);
		FileScan initialScan1 = new FileScan(initialSchema1, initialFile1);

		Iterator currentJoin;
		if (tables.length > 1)
		{
			Schema initialSchema2 = Minibase.SystemCatalog.getSchema(tables[1]);
			HeapFile initialFile2 = new HeapFile(tables[1]);
			FileScan initialScan2 = new FileScan(initialSchema2, initialFile2);

			currentJoin = new SimpleJoin(initialScan1, initialScan2,
					(Predicate[]) null);

			for (int i = 2; i < tables.length; i++)
			{
				Schema currentScehma = Minibase.SystemCatalog
						.getSchema(tables[i]);
				HeapFile currentFile = new HeapFile(tables[i]);
				FileScan currentScan = new FileScan(currentScehma, currentFile);

				SimpleJoin nextJoin = new SimpleJoin(currentJoin, currentScan,
						(Predicate[]) null);
				currentJoin = nextJoin;
			}

		} else
		{
			currentJoin = initialScan1;
		}

		// Select applicable tuples
		try
		{
			CNFSelection selection = new CNFSelection(currentJoin, predicates);
			
			// project on specific columns (if projection columns exist)
			if (columns.length > 0)
			{
				Integer[] prj_columns = new Integer[columns.length];
				
				for (int i = 0; i < prj_columns.length; i++)
				{
					prj_columns[i] = (Integer) selection.getSchema()
							.fieldNumber(columns[i]);
				}
				
				Projection projection = new Projection(selection, prj_columns);
				return projection.execute();
				
			} else
			{
				return selection.execute();
			}
			
		} catch (Exception e)
		{
			System.out.println(">>>>>>>>>> FATAL ERROR : " + e.getMessage());
			return 0;
		}

	}
	
	

} // class Select implements Plan
