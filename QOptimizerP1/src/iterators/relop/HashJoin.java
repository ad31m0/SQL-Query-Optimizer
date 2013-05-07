package iterators.relop;

import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import iterators.Iterator;
import iterators.scanners.FileScan;
import iterators.scanners.KeyScan;
import primitives.Schema;
import primitives.Tuple;

public class HashJoin extends Iterator {

	private Iterator left, right;
	private HeapFile file;
	private HashIndex index;
	
	private KeyScan scan;
	
	private Tuple left_tuple, right_tuple, next;
	
	
	
	private int i;
	
	public HashJoin(Iterator left, Iterator right, int i, int j) {
		this.left = left;
		this.right = right;
		this.i = i;
		
		setSchema(Schema.join(left.getSchema(), right.getSchema()));
		initHashIndex(j);
	}

	private void initHashIndex(int colno){
		index = new HashIndex(null);
		file = new HeapFile(null);
		Tuple tuple;
		while(right.hasNext()){
			tuple = right.getNext();
			SearchKey key = new SearchKey(tuple.getField(colno));
			RID rid = file.insertRecord(tuple.getData());
			index.insertEntry(key, rid);
		}
	}
	@Override
	public void explain(int depth) {
		for(int i=0; i<depth; i++)
			System.out.print("\t");
		System.out.println(">HashJoin Iterator");
		left.explain(depth+1);
		right.explain(depth+1);
	}

	@Override
	public void restart() {
		left.restart();
	}

	@Override
	public boolean isOpen() {
		return left.isOpen();
	}

	@Override
	public void close() {
		left.close();
		right.close();
		index = null;
		file = null;
	}

	@Override
	public boolean hasNext() {
		boolean match = false;
		if(scan!=null && scan.hasNext()){
			right_tuple = scan.getNext();
			next = Tuple.join(left_tuple, right_tuple, getSchema());
			return true;
		}
		while(!match && left.hasNext()){
			left_tuple = left.getNext();
			SearchKey key = new SearchKey(left_tuple.getField(i));
			scan = new KeyScan(right.getSchema(), index, key, file);
			if(scan.hasNext()){
				right_tuple = scan.getNext();
				next = Tuple.join(left_tuple, right_tuple, getSchema());
				return true;
			}
		}
		return false;
	}

	@Override
	public Tuple getNext() {
		return next;
	}

}
