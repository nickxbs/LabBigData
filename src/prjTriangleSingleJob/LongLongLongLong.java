package prjTriangleSingleJob;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.*;

public class LongLongLongLong implements WritableComparable<LongLongLongLong> {

	private Text _rel;

	private IntWritable _first;
	private IntWritable _second;
	private IntWritable _third;
	private IntWritable _fourth;
	

public void set(Text rel, IntWritable first, IntWritable second,IntWritable third,IntWritable fourth) {
	_rel=rel;
	_first=first;
	_second=second;
	_third=third;
	_fourth=fourth;
}
public Text getRel() {
return _rel;
}
public IntWritable getFirst() {
return _first;
}

public IntWritable getSecond() {
return _second;
}
public IntWritable getthird() {
return _third;
}
public IntWritable getfourth() {
return _fourth;
}
  
public LongLongLongLong() {
	_rel=new Text();
	_first= new IntWritable();
	_second= new IntWritable();
	_third= new IntWritable();
	_fourth= new IntWritable();
}

public LongLongLongLong(String rel,Integer first, Integer second,Integer third,Integer fourth) {
	  this.set(new Text(rel),new IntWritable(first), new IntWritable(second), new IntWritable(third), new IntWritable(fourth));
	}

@Override
public void write(DataOutput out) throws IOException {
	  _rel.write(out);
	  _first.write(out);
	  _second.write(out);
	  _third.write(out);
	  _fourth.write(out);
}

@Override
public void readFields(DataInput in) throws IOException {
	  _rel.readFields(in);
	  _first.readFields(in);
	  _second.readFields(in);
	  _third.readFields(in);
	  _fourth.readFields(in);

}

@Override
public int hashCode() {
	return _first.hashCode() * 163 + _second.hashCode();
}

@Override
public boolean equals(Object o) {
 if (o instanceof LongLongLongLong) {
   LongLongLongLong tp = (LongLongLongLong) o;
   return _first.equals(tp.getFirst()) && _second.equals(tp.getSecond()) && _third.equals(tp.getthird()) && _fourth.equals(tp.getfourth()) ;
 }
 return false;
}

@Override
public String toString() {
 return _first + "\t" + _second+ "\t" +_third;
}

@Override
public int compareTo(LongLongLongLong tp) {
		   LongLongLongLong la=this;
		   LongLongLongLong lb=tp;
		   
		  if(!la.getFirst().equals(lb.getFirst()))
			  return (la.getFirst().compareTo(lb.getFirst()));
		  else{
			  if(!la.getSecond().equals(lb.getSecond()))
				  return (la.getSecond().compareTo(lb.getSecond()));
			  else{
				  if(!la.getthird().equals(lb.getthird()))
					  return (la.getthird().compareTo(lb.getthird()));
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
    super(LongLongLongLong.class);
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
 WritableComparator.define(LongLongLongLong.class, new Comparator());
}


}
