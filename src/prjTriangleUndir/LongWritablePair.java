package prjTriangleUndir;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;



public class LongWritablePair implements WritableComparable<LongWritablePair> {


	private LongWritable _first;
	private LongWritable _second;

public void set(LongWritable first, LongWritable second) {
	_first=first;
	_second=second;
}

public LongWritable getFirst() {
return _first;
}

public LongWritable getSecond() {
return _second;
}
  
public LongWritablePair() {
	_first= new LongWritable();
	_second= new LongWritable();
}

public LongWritablePair(Long first, Long second) {
	  this.set(new LongWritable(first), new LongWritable(second));
	}
public LongWritablePair(int first, int second) {
	  this.set(new LongWritable(first), new LongWritable(second));
	}
public LongWritablePair(String first, String second) {
	  this.set(new LongWritable(Long.parseLong(first)), new LongWritable(Long.parseLong(second)));
	}

public LongWritablePair(LongWritable first, LongWritable second) {
  this.set(first, second);
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
 if (o instanceof LongWritablePair) {
   LongWritablePair tp = (LongWritablePair) o;
   return _first.equals(tp.getFirst()) && _second.equals(tp.getSecond());
 }
 return false;
}

@Override
public String toString() {
 return _first + "\t" + _second;
}

@Override
public int compareTo(LongWritablePair tp) {
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
    super(LongWritablePair.class);
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
 WritableComparator.define(LongWritablePair.class, new Comparator());
}

/** Compare just the first element of the Pair */
public static class FirstComparator extends WritableComparator {
 
 private static final LongWritable.Comparator LongWritable_COMPARATOR = new LongWritable.Comparator();
 
 public FirstComparator() {
   super(LongWritablePair.class);
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
   if (a instanceof LongWritablePair && b instanceof LongWritablePair) {
     return ((LongWritablePair) a).getFirst().compareTo(((LongWritablePair) b).getFirst());
   }
   return super.compare(a, b);
 }

}
}
