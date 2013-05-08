package query;

import global.AttrType;
import global.Minibase;
import parser.AST_Describe;
import primitives.Schema;

/**
 * Execution plan for describing tables.
 */
class Describe implements Plan
{

	private String table_name;
	public static final int MAX_BADDING = 20;

	/**
	 * Optimizes the plan, given the parsed query.
	 * 
	 * @throws QueryException
	 *             if table doesn't exist
	 */
	public Describe(AST_Describe tree) throws QueryException
	{

		table_name = tree.getFileName();
		QueryCheck.tableExists(table_name);
	} // public Describe(AST_Describe tree) throws QueryException

	/**
	 * Executes the plan and prints applicable output.
	 */
	public void execute()
	{

		Schema schema = Minibase.SystemCatalog.getSchema(table_name);
		int num_of_fields = schema.getCount();

		System.out.println("Table :" + table_name);
		System.out.println("------------------------------------------------------------");

		
		System.out.print("Field Number:");
		for (int i = 0; i < MAX_BADDING - "Field Number:".length(); i++)
		{
			System.out.print(" ");
		}

		System.out.print("Field Name:");
		for (int i = 0; i < MAX_BADDING - "Field Name:".length(); i++)
		{
			System.out.print(" ");
		}

		System.out.print("Field Type:");
		for (int i = 0; i < MAX_BADDING - "Field Type:".length(); i++)
		{
			System.out.print(" ");
		}
		
		System.out
				.println("\n------------------------------------------------------------");

		for (int j = 0; j < num_of_fields; j++)
		{
			System.out.print(j + 1 + "");
			for (int i = 0; i < MAX_BADDING - (j + 1 + "").length(); i++)
			{
				System.out.print(" ");
			}

			System.out.print(schema.fieldName(j));
			for (int i = 0; i < MAX_BADDING - schema.fieldName(j).length(); i++)
			{
				System.out.print(" ");
			}

			System.out.print(getFieldType(schema, j));
			for (int i = 0; i < MAX_BADDING - getFieldType(schema, j).length(); i++)
			{
				System.out.print(" ");
			}
			System.out.println("");
		}

	} // public void execute()

	private String getFieldType(Schema schema, int fieldno)
	{
		int type = schema.fieldType(fieldno);
		String field_type;
		switch (type)
		{
		case AttrType.INTEGER:
			field_type = "INTEGER";
		break;

		case AttrType.FLOAT:
			field_type = "FLOAT";
		break;
		
		case AttrType.STRING:
			field_type = "STRING";
		break;
		default:
			field_type = "UNKNOWN TYPE";
		break;
		}
		
		return field_type;
	}
} // class Describe implements Plan
