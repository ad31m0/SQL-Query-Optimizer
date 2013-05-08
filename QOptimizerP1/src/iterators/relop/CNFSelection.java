package iterators.relop;

import iterators.Iterator;
import primitives.Predicate;
import primitives.Tuple;

/**
 * The selection operator specifies which tuples to retain under a condition; in
 * Mini-base, this condition is simply a set of independent predicates logically
 * connected by AND operators.
 */
public class CNFSelection extends Iterator
{

	private Iterator iterator;
	private Predicate[][] predicates;
	Tuple current_tuple;
	boolean is_consumed;
	boolean has_next;
	boolean is_open;

	/**
	 * Constructs a selection, given the underlying iterator and predicates.
	 * 
	 * @throws Exception
	 */
	public CNFSelection(Iterator iter, Predicate[]... preds) throws Exception
	{

		// validate predicates
		
		for (int i = 0; i < preds.length; i++)
		{
			for(int j=0 ; j < preds[0].length ; j++)
			{
				if (!preds[i][j].validate(iter.getSchema()))
				{
					throw new Exception(
							"Invalid Predicate Exception @ Predicate : "
									+ preds[i].toString());
				}
			}
		}

		// initialize the selection schema and iterator
		this.iterator = iter;
		this.predicates = preds;
		setSchema(iter.getSchema());

		is_consumed = true;
		has_next = false;
		is_open = true;
	}

	/**
	 * Gives a one-line explanation of the iterator, repeats the call on any
	 * child iterators, and increases the indent depth along the way.
	 */
	public void explain(int depth)
	{
		for (int i = 0; i < depth; i++)
		{
			System.out.print("\t\t");
		}
		System.out.print("Selection Iterator:\n");
		iterator.explain(depth + 1);
	}

	/**
	 * Restarts the iterator, i.e. as if it were just constructed.
	 */
	public void restart()
	{
		iterator.restart();
		is_consumed = true;
		has_next = false;
	}

	/**
	 * Returns true if the iterator is open; false otherwise.
	 */
	public boolean isOpen()
	{
		return is_open;
	}

	/**
	 * Closes the iterator, releasing any resources (i.e. pinned pages).
	 */
	public void close()
	{
		iterator.close();
		is_open=false;
	}

	/**
	 * Returns true if there are more tuples, false otherwise.
	 */
	public boolean hasNext()
	{
		if (is_consumed)
		{
			boolean matches = true;
			has_next = false ;
			while (iterator.hasNext())
			{
				current_tuple = iterator.getNext();

				matches = true;
				for (int i = 0; i < predicates.length; i++)
				{
					if (!checkTuple(predicates[i]))
					{
						matches = false;
						break;
					}
				}

				if (matches)
				{
					has_next = true;
					break;
				}
			}
			is_consumed = false;
		}
		return has_next;
	}

	/**
	 * Gets the next tuple in the iteration.
	 * 
	 * @throws IllegalStateException
	 *             if no more tuples
	 */
	public Tuple getNext()
	{
		if(hasNext())
		{
			is_consumed = true;
			return current_tuple;
		}
		else
		{
			throw new IllegalStateException("Iterator has no more tuples");
		}
	}

	private boolean checkTuple(Predicate[] predicates)
	{
		boolean matches = false;
		for(int i = 0 ; i < predicates.length ; i++)
		{
			if(predicates[i].evaluate(current_tuple))
			{
				matches = true;
				break;
			}
		}
		return matches;
	}
} // public class Selection extends Iterator
