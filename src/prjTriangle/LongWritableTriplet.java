package prjTriangle;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;



public class LongWritableTriplet implements WritableComparable<LongWritableTriplet> {


	private LongWritable _first;
	private LongWritable _second;
	private LongWritable _ter;
	

public void set(LongWritable first, LongWritable second,LongWritable ter) {
	_first=first;
	_second=second;
	_ter=ter;
}

public LongWritable getFirst() {
return _first;
}

public LongWritable getSecond() {
return _second;
}
public LongWritable getTer() {
return _ter;
}
  
public LongWritableTriplet() {
	_first= new LongWritable();
	_second= new LongWritable();
	_ter= new LongWritable();
}

public LongWritableTriplet(Long first, Long second,Long ter) {
	  this.set(new LongWritable(first), new LongWritable(second), new LongWritable(ter));
	}
public LongWritableTriplet(LongWritable first, LongWritable second,LongWritable ter) {
  this.set(first, second,ter);
}

@Override
public void write(DataOutput out) throws IOException {
	  _first.write(out);
	  _second.write(out);
	  _ter.write(out);
}

@Override
public void readFields(DataInput in) throws IOException {
	  _first.readFields(in);
	  _second.readFields(in);
	  _ter.readFields(in);
}

@Override
public int hashCode() {
	return _first.hashCode() * 163 + _second.hashCode();
}

@Override
public boolean equals(Object o) {
 if (o instanceof LongWritableTriplet) {
   LongWritableTriplet tp = (LongWritableTriplet) o;
   return _first.equals(tp.getFirst()) && _second.equals(tp.getSecond()) && _ter.equals(tp.getTer());
 }
 return false;
}

@Override
public String toString() {
 return _first + "\t" + _second+ "\t" +_ter;
}

@Override
public int compareTo(LongWritableTriplet tp) {
 int cmp = _first.compareTo(tp.getFirst());
 if (cmp != 0) {
   return cmp;
 }
 cmp = _second.compareTo(tp.getSecond());
 if (cmp != 0) {
   return cmp;
 }

 return _ter.compareTo(tp.getTer());
}



// DO NOT TOUCH THE CODE BELOW

/** Compare two pairs based on their values */
public static class Comparator extends WritableComparator {
 
  /** Reference to standard Hadoop LongWritable comparator */
  private static final LongWritable.Comparator LongWritable_COMPARATOR = new LongWritable.Comparator();
 
  public Comparator() {
    super(LongWritableTriplet.class);
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
 WritableComparator.define(LongWritableTriplet.class, new Comparator());
}

/** Compare just the first element of the Pair */
public static class FirstComparator extends WritableComparator {
 
 private static final LongWritable.Comparator LongWritable_COMPARATOR = new LongWritable.Comparator();
 
 public FirstComparator() {
   super(LongWritableTriplet.class);
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
 
@Override
@SuppressWarnings("itsOK ")
 public int compare(WritableComparable a, WritableComparable b) {
   if (a instanceof LongWritableTriplet && b instanceof LongWritableTriplet) {
     return ((LongWritableTriplet) a).getFirst().compareTo(((LongWritableTriplet) b).getFirst());
   }
   return super.compare(a, b);
 }

}
}
