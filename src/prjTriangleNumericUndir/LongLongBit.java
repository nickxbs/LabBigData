package prjTriangleNumericUndir;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

public class LongLongBit implements WritableComparable<LongLongBit> {


	private LongWritable _first;
	private LongWritable _second;
	private BooleanWritable _ter;
	

public void set(LongWritable first, LongWritable second,BooleanWritable ter) {
	_first=first;
	_second=second;
	_ter=ter;
}

public void set(Long first, Long second,Boolean ter) {
	  this.set(new LongWritable(first), new LongWritable(second), new BooleanWritable(ter));
	}
public LongWritable getFirst() {
return _first;
}

public LongWritable getSecond() {
return _second;
}
public BooleanWritable getTer() {
return _ter;
}
  
public LongLongBit() {
	_first= new LongWritable();
	_second= new LongWritable();
	_ter= new BooleanWritable();
}

public LongLongBit(Long first, Long second,Boolean ter) {
	  this.set(new LongWritable(first), new LongWritable(second), new BooleanWritable(ter));
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
 if (o instanceof LongLongBit) {
   LongLongBit tp = (LongLongBit) o;
   return _first.equals(tp.getFirst()) && _second.equals(tp.getSecond()) && _ter.equals(tp.getTer());
 }
 return false;
}

@Override
public String toString() {
 return _first + "\t" + _second+ "\t" +_ter;
}

@Override
public int compareTo(LongLongBit tp) {
		   LongLongBit la=this;
		   LongLongBit lb=tp;
		   
		  if(!la.getFirst().equals(lb.getFirst()))
			  return (la.getFirst().compareTo(lb.getFirst()));
		  else{
			  if(!la.getSecond().equals(lb.getSecond()))
				  return (la.getSecond().compareTo(lb.getSecond()));
			  else{
				  if(!la.getTer().equals(lb.getTer()))
					  return (la.getTer().compareTo(lb.getTer()));
			  	}

		  	}
		  return 1;
		  
}



// DO NOT TOUCH THE CODE BELOW

/** Compare two pairs based on their values */
public static class Comparator extends WritableComparator {
 
  /** Reference to standard Hadoop LongWritable comparator */
  private static final LongWritable.Comparator LongWritable_COMPARATOR = new LongWritable.Comparator();
 
  public Comparator() {
    super(LongLongBit.class);
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
 WritableComparator.define(LongLongBit.class, new Comparator());
}


}
