package prjTriangleWiki;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

/**
 * TextPair is a Pair of Text that is Writable (Hadoop serialization API) and
 * Comparable to itself.
 * 
 */
public class TextPair implements WritableComparable<TextPair> {

	private Text _first;
	private Text _second;

	public void set(Text first, Text second) {
		_first = first;
		_second = second;
	}
	public void set(String first, String second) {
		_first.set(first);
		_second.set(second);
	}
	public Text getFirst() {
		return _first;
	}

	public Text getSecond() {
		return _second;
	}

	public TextPair() {
		_first = new Text();
		_second = new Text();
	}

	public TextPair(String first, String second) {
		this.set(new Text(first), new Text(second));
	}

	public TextPair(Text first, Text second) {
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
		if (o instanceof TextPair) {
			TextPair tp = (TextPair) o;
			return _first.equals(tp.getFirst())
					&& _second.equals(tp.getSecond());
		}
		return false;
	}

	@Override
	public String toString() {
		return _first + "\t" + _second;
	}

	@Override
	public int compareTo(TextPair tp) {
		int cmp = _first.compareTo(tp.getFirst());
		if (cmp != 0) {
			return cmp;
		}
		return _second.compareTo(tp.getSecond());
	}

	// DO NOT TOUCH THE CODE BELOW

	/** Compare two pairs based on their values */
	public static class Comparator extends WritableComparator {

		/** Reference to standard Hadoop Text comparator */
		private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

		public Comparator() {
			super(TextPair.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {

			try {
				int firstL1 = WritableUtils.decodeVIntSize(b1[s1])
						+ readVInt(b1, s1);
				int firstL2 = WritableUtils.decodeVIntSize(b2[s2])
						+ readVInt(b2, s2);
				int cmp = TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2,
						firstL2);
				if (cmp != 0) {
					return cmp;
				}
				return TEXT_COMPARATOR.compare(b1, s1 + firstL1, l1 - firstL1,
						b2, s2 + firstL2, l2 - firstL2);
			} catch (IOException e) {
				throw new IllegalArgumentException(e);
			}
		}
	}

	static {
		WritableComparator.define(TextPair.class, new Comparator());
	}
}