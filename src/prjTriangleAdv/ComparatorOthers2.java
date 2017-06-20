package prjTriangleAdv;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Compare just the first element of the Pair
 */
public class ComparatorOthers2 extends WritableComparator {

    public ComparatorOthers2() {
        super(KeyClosure.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        if (a instanceof KeyClosure && b instanceof KeyClosure) {
            KeyClosure la = (KeyClosure) a;
            KeyClosure lb = (KeyClosure) b;

            return la.compareTo(lb);

        }
        return super.compare(a, b);
    }

}