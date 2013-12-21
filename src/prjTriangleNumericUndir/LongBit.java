package prjTriangleNumericUndir;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;



public class LongBit implements WritableComparable<LongBit> {


	private LongWritable _first;
	private BooleanWritable _second;

public void set(LongWritable first, BooleanWritable second) {
	_first=first;
	_second=second;
}

public void set(Long first, Boolean second) {
	 this.set(new LongWritable(first), new BooleanWritable(second));
	 }

public LongWritable getFirst() {
return _first;
}

public BooleanWritable getSecond() {
return _second;
}
  
public LongBit() {
	_first= new LongWritable();
	_second= new BooleanWritable();
}

public LongBit(Long first, Boolean second) {
	  this.set(new LongWritable(first), new BooleanWritable(second));
	}



@Override
public void write(DataOutput out) throws IOException {
	  _first.write(out);
	  _second.write(out);
}

@Override
public void readFields(DataInput in) throws IOException {
	  _first.readFields(in);
	  _second.readFields(in);
}

@Override
public int hashCode() {
	return _first.hashCode() * 163 + _second.hashCode();
}

@Override
public boolean equals(Object o) {
 if (o instanceof LongBit) {
   LongBit tp = (LongBit) o;
   return _first.equals(tp.getFirst()) && _second.equals(tp.getSecond());
 }
 return false;
}

@Override
public String toString() {
 return _first + "\t" + _second;
}

@Override
public int compareTo(LongBit tp) {
 int cmp = _first.compareTo(tp.getFirst());
 if (cmp != 0) {
   return cmp;
 }
 return _second.compareTo(tp.getSecond());
}




// DO NOT TOUCH THE CODE BELOW

/** Compare two pairs based on their values */
public static class Comparator extends WritableComparator {
 
  /** Reference to standard Hadoop LongWritable comparator */
  private static final LongWritable.Comparator LongWritable_COMPARATOR = new LongWritable.Comparator();
 
  public Comparator() {
    super(LongBit.class);
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
 WritableComparator.define(LongBit.class, new Comparator());
}

/** Compare just the first element of the Pair */
public static class FirstComparator extends WritableComparator {
 
 private static final LongWritable.Comparator LongWritable_COMPARATOR = new LongWritable.Comparator();
 
 public FirstComparator() {
   super(LongBit.class);
 }

 @Override
 public int compare(byte[] b1, int s1, int l1,
                    byte[] b2, int s2, int l2) {
   
   try {
     int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
     int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
     return LongWritable_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
   } catch (IOException e) {
     throw new IllegalArgumentException(e);
   }
 }
 
@SuppressWarnings("unchecked")
@Override
 public int compare(WritableComparable a, WritableComparable b) {
   if (a instanceof LongBit && b instanceof LongBit) {
     return ((LongBit) a).getFirst().compareTo(((LongBit) b).getFirst());
   }
   return super.compare(a, b);
 }

}
}
