package prjTriangleAdv;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BucketItem implements WritableComparable<BucketItem> {

	private Text _typeRel;
	private IntWritable _bucketIndex;
	private IntWritable _from;
	private IntWritable _fromDegree;


	public Text getTypeRel() {
		return _typeRel;
	}
	public IntWritable getBucketIndex() {
		return _bucketIndex;
	}
	public IntWritable getFrom() {
		return _from;
	}
	public IntWritable getFromDegree() {
		return _fromDegree;
	}

	public BucketItem() {
		_typeRel=new Text();
		_bucketIndex= new IntWritable();
		_from= new IntWritable();
		_fromDegree= new IntWritable();

	}

	public BucketItem(String rel,int indexBucket, int from, int fromDegree) {
		this.set(new Text(rel),  new IntWritable(indexBucket), new IntWritable(from),new IntWritable(fromDegree));
	}

	public void set(Text rel, IntWritable indexBucket, IntWritable from, IntWritable fromDegree) {
		_typeRel=rel;
		_bucketIndex=indexBucket;
		_from=from;
		_fromDegree=fromDegree;

	}

	public void write(DataOutput out) throws IOException {
		_typeRel.write(out);
		_bucketIndex.write(out);
		_from.write(out);
		_fromDegree.write(out);
	}


	public void readFields(DataInput in) throws IOException {
		_typeRel.readFields(in);
		_bucketIndex.readFields(in);
		_from.readFields(in);
		_fromDegree.readFields(in);
	}

	public int hashCode() {
		return _bucketIndex.hashCode() * 163* 163*163 + _from.hashCode()*163 * 163+_fromDegree.hashCode()*163 + _typeRel.hashCode();
	}


	public boolean equals(Object o) {
		if (o instanceof BucketItem) {
			BucketItem tp = (BucketItem) o;
			return _bucketIndex.equals(tp.getBucketIndex()) && _from.equals(tp.getFrom()) && _typeRel.equals(tp.getTypeRel()) ;
		}
		return false;
	}


	public String toString() {
		return _bucketIndex + "\t" + _from;
	}


	public int compareTo(BucketItem tp) {
		BucketItem la=this;
		BucketItem lb=tp;

		if(!la.getBucketIndex().equals(lb.getBucketIndex()))
			return (la.getBucketIndex().compareTo(lb.getBucketIndex()));
		else{
			if(!la.getFromDegree().equals(lb.getFromDegree()))
				return (la.getFromDegree().compareTo(lb.getFromDegree()));
			else{
				if(!la.getFrom().equals(lb.getFrom()))
					return (la.getFrom().compareTo(lb.getFrom()));
			}
		}
		return 1;
	}



// DO NOT TOUCH THE CODE BELOW

	/** Compare two pairs based on their values */
	public static class Comparator extends WritableComparator {

		/** Reference to standard Hadoop LongWritable comparator */
		private static final IntWritable.Comparator LongWritable_COMPARATOR = new IntWritable.Comparator();

		public Comparator() {
			super(BucketItem.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1,
						   byte[] b2, int s2, int l2) {

			try {
				int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
				int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
				int cmp = LongWritable_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
				if (cmp != 0) {
					return cmp;
				}
				return LongWritable_COMPARATOR.compare(b1, s1 + firstL1, l1 - firstL1,
						b2, s2 + firstL2, l2 - firstL2);
			} catch (IOException e) {
				throw new IllegalArgumentException(e);
			}
		}
	}

	static {
		WritableComparator.define(BucketItem.class, new Comparator());
	}


}
