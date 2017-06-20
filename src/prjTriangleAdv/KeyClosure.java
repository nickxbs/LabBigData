package prjTriangleAdv;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class KeyClosure implements WritableComparable<KeyClosure> {


	private IntWritable _bucketIndex;
	private IntWritable _b;
	private IntWritable _c;
	private IntWritable _value;


	public Integer getBucketIndex() {
		return new Integer(_bucketIndex.get());
	}
	public Integer getB() {
		return new Integer(_b.get());
	}
	public Integer getC() {
		return new Integer(_c.get());
	}
	public Integer getValue() {
		return new Integer(_value.get());
	}

	public KeyClosure() {

		_bucketIndex= new IntWritable();
		_b = new IntWritable();
		_c = new IntWritable();
		_value = new IntWritable();
	}

	public KeyClosure(int indexBucket, int b, int c, int value) {
		this.set(new IntWritable(indexBucket), new IntWritable(b),new IntWritable(c), new IntWritable(value));
	}

	public void set(IntWritable indexBucket, IntWritable b, IntWritable c, IntWritable value) {
		_bucketIndex=indexBucket;
		_b =b;
		_c =c;
		_value=value;
	}

	public void write(DataOutput out) throws IOException {
		_bucketIndex.write(out);
		_b.write(out);
		_c.write(out);
		_value.write(out);
	}


	public void readFields(DataInput in) throws IOException {
		_bucketIndex.readFields(in);
		_b.readFields(in);
		_c.readFields(in);
		_value.readFields(in);
	}

	public int hashCode() {
		return _bucketIndex.hashCode() * 163* 163*163+ _b.hashCode()*163*163+ _c.hashCode()*163+_value.hashCode();
	}


	public boolean equals(Object o) {
		if (o instanceof KeyClosure) {
			KeyClosure tp = (KeyClosure) o;
			return _bucketIndex.equals(tp.getBucketIndex()) && _b.equals(tp.getB()) ;
		}
		return false;
	}


	public String toString() {
		return _bucketIndex + "\t" + _b;
	}


	public int compareTo(KeyClosure tp) {
		KeyClosure la=this;
		KeyClosure lb=tp;

		if(!la.getBucketIndex().equals(lb.getBucketIndex()))
			return (la.getBucketIndex().compareTo(lb.getBucketIndex()));
		else{
			if(!la.getB().equals(lb.getB()))
				return (la.getB().compareTo(lb.getB()));
			else{
				if(!la.getC().equals(lb.getC()))
					return (la.getC().compareTo(lb.getC()));
				else{
					if(!la.getValue().equals(lb.getValue()))
						return (la.getValue().compareTo(lb.getValue()));
				}

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
			super(KeyClosure.class);
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
		WritableComparator.define(KeyClosure.class, new Comparator());
	}


}
