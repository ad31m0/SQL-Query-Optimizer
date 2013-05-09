package query;

import java.util.ArrayList;

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
	private Iterator[] scans;
	ArrayList<Predicate[]> predicates_list ;
	
	

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
		
		predicates_list = new ArrayList<Predicate[]>();
		for (int i = 0; i < predicates.length; i++)
		{
			predicates_list.add(predicates[i]);
		}
		
		validateQuery();

		scans = new Iterator[tables.length];

		for (int i = 0; i < tables.length; i++)
		{
			Schema schema = Minibase.SystemCatalog.getSchema(tables[i]);
			HeapFile file = new HeapFile(tables[i]);
			FileScan scan = new FileScan(schema, file);
			Predicate[][] opt_predicates = getOptPredicates(schema);
			if (opt_predicates.length > 0)
			{
				try
				{
					CNFSelection optimized_selection = new CNFSelection(scan,
							opt_predicates);
					scans[i] = optimized_selection;
				} catch (Exception e)
				{
					e.printStackTrace();
				}
			} else
			{
				scans[i] = scan;
			}
		}
		
		predicates = new Predicate[predicates_list.size()][0];
		for (int i = 0; i < predicates.length; i++)
		{
			predicates[i] = predicates_list.get(i);
		}
	} // public Select(AST_Select tree) throws QueryException

	private void validateQuery() throws QueryException
	{
		// Check tables existence in the database

		Schema initial_schema1 = QueryCheck.tableExists(tables[0]);
		Schema joined_schema;
		if (tables.length > 1)
		{
			Schema initial_schema2 = QueryCheck.tableExists(tables[1]);
			joined_schema = Schema.join(initial_schema1, initial_schema2);
			for (int i = 2; i < tables.length; i++)
			{
				Schema next_schema = Schema.join(joined_schema,
						QueryCheck.tableExists(tables[i]));
				joined_schema = next_schema;
			}
		} else
		{
			joined_schema = initial_schema1;
		}

		// Check columns existence in the joined schema

		for (int i = 0; i < columns.length; i++)
		{
			QueryCheck.columnExists(joined_schema, columns[i]);
		}

		// Check predicates validity on the joined schema

		for (int i = 0; i < predicates.length; i++)
		{
			for (int j = 0; j < predicates[0].length; j++)
			{
				if (!predicates[i][j].validate(joined_schema))
				{
					throw new QueryException("Invalid Predicate : @ "
							+ predicates[i][j].toString());
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
		int cnt;
		try
		{
			cnt = executeOptimizedPlan();
			System.out.println(cnt + " rows selected");
		} catch (Exception e)
		{
			e.printStackTrace();
		}
	} // public void execute()

	
	private int executeOptimizedPlan() throws Exception
	{
		int cnt = 0;
		// Get joined schema
		Iterator currentJoin;
		if (tables.length > 1)
		{
			currentJoin = new SimpleJoin(scans[0], scans[1], (Predicate[]) null);

			for (int i = 2; i < tables.length; i++)
			{
				SimpleJoin nextJoin = new SimpleJoin(currentJoin, scans[i],
						(Predicate[]) null);
				currentJoin = nextJoin;
			}

		} else
		{
			currentJoin = scans[0];
		}

		// Select applicable tuples
		CNFSelection selection = new CNFSelection(currentJoin, predicates);

		// project on specific columns (if projection columns exist)
		if (columns.length > 0)
		{
			Integer[] prj_columns = new Integer[columns.length];

			for (int i = 0; i < prj_columns.length; i++)
			{
				prj_columns[i] = (Integer) selection.getSchema().fieldNumber(
						columns[i]);
			}

			Projection projection = new Projection(selection, prj_columns);
			cnt = projection.execute();
			projection.close();
			return cnt;

		} else
		{
			cnt = selection.execute();
			selection.close();
			return cnt;
		}

	}

	private Predicate[][] getOptPredicates(Schema schema)
	{
		ArrayList<Predicate[]> result_list = new ArrayList<Predicate[]>();
		boolean matches = true;
		for (int i = 0; i < predicates_list.size(); i++)
		{
			matches = true;
			for (int j = 0; j < predicates_list.get(i).length; j++)
			{
				if (!predicates_list.get(i)[j].validate(schema))
				{
					matches = false;
					break;
				}
			}
			if (matches)
			{
				result_list.add(predicates_list.get(i));
				predicates_list.remove(i);
			}
		}
		Predicate[][] result = new Predicate[result_list.size()][0];
		for (int i = 0; i < result.length; i++)
		{
			result[i] = result_list.get(i);
		}
		return result;
	}

} // class Select implements Plan
