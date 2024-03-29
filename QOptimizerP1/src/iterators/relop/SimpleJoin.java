package iterators.relop;

import iterators.Iterator;
import primitives.Predicate;
import primitives.Schema;
import primitives.Tuple;

/**
 * The simplest of all join algorithms: nested loops (see textbook, 3rd edition,
 * section 14.4.1, page 454).
 */
public class SimpleJoin extends Iterator {

	private Iterator left, right;
	private Tuple left_tuple, right_tuple;
	private Tuple next_tuple;
	private Predicate[] preds;

	/**
	 * Constructs a join, given the left and right iterators and join predicates
	 * (relative to the combined schema).
	 */
	public SimpleJoin(Iterator left, Iterator right, Predicate... preds) {
		this.left = left;
		this.right = right;
		this.preds = preds;
		if(left.hasNext())
		{
			this.left_tuple = left.getNext();
		}
		Schema schema = Schema.join(left.getSchema(), right.getSchema());
		this.setSchema(schema);
	}

	/**
	 * Gives a one-line explaination of the iterator, repeats the call on any
	 * child iterators, and increases the indent depth along the way.
	 */
	public void explain(int depth) {
		for (int i = 0; i < depth; i++)
			System.out.print("\t");
		System.out.println("SimpleJoin Iterator");
		left.explain(depth + 1);
		right.explain(depth + 1);
	}

	/**
	 * Restarts the iterator, i.e. as if it were just constructed.
	 */
	public void restart() {
		left.restart();
		right.restart();
	}

	/**
	 * Returns true if the iterator is open; false otherwise.
	 */
	public boolean isOpen() {
		return left.isOpen() && right.isOpen();
	}

	/**
	 * Closes the iterator, releasing any resources (i.e. pinned pages).
	 */
	public void close() {
		left.close();
		right.close();
	}

	/**
	 * Returns true if there are more tuples, false otherwise.
	 */
	public boolean hasNext() {
		boolean match = false;
		Tuple joined_tuple;
		while (!match) {

			if (!right.hasNext()) {
				right.restart();
				if (!left.hasNext())
				{
					this.close();
					return false;
				}
				left_tuple = left.getNext();
			}
			if(!right.hasNext() || left_tuple == null)
			{
				this.close();
				return false;
			}
			right_tuple = right.getNext();
			joined_tuple = Tuple.join(left_tuple, right_tuple, getSchema());
			
			if(preds==null || preds.length==0){
				next_tuple = joined_tuple;
				return true;
			}
			for (Predicate pred : preds)
				if (pred.evaluate(joined_tuple)) {
					next_tuple = joined_tuple;
					return true;
				}
		}
		this.close();
		return false;
	}

	/**
	 * Gets the next tuple in the iteration.
	 * 
	 * @throws IllegalStateException
	 *             if no more tuples
	 */
	public Tuple getNext() {
		return next_tuple;
	}

} // public class SimpleJoin extends Iterator
