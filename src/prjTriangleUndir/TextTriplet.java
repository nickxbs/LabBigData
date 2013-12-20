package prjTriangleUndir;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

public class TextTriplet implements WritableComparable<TextTriplet> {


	private Text _first;
	private Text _second;
	private Text _ter;
	

public void set(Text first, Text second,Text ter) {
	_first=first;
	_second=second;
	_ter=ter;
}

public Text getFirst() {
return _first;
}

public Text getSecond() {
return _second;
}
public Text getTer() {
return _ter;
}
  
public TextTriplet() {
	_first= new Text();
	_second= new Text();
	_ter= new Text();
}

public TextTriplet(String first, String second,String ter) {
	  this.set(new Text(first), new Text(second), new Text(ter));
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
 if (o instanceof TextTriplet) {
   TextTriplet tp = (TextTriplet) o;
   return _first.equals(tp.getFirst()) && _second.equals(tp.getSecond()) && _ter.equals(tp.getTer());
 }
 return false;
}

@Override
public String toString() {
 return _first + "\t" + _second+ "\t" +_ter;
}

@Override
public int compareTo(TextTriplet tp) {
		   TextTriplet la=this;
		   TextTriplet lb=tp;
		   
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
    super(TextTriplet.class);
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
 WritableComparator.define(TextTriplet.class, new Comparator());
}


}
