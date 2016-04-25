package InvertedIndex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class InvertedIndexComparator extends WritableComparator {

	public InvertedIndexComparator() {
		super(InvertedKey.class, true);
	}


	public int compare(WritableComparable o1, WritableComparable o2) {
			InvertedKey key1 = (InvertedKey) o1;
			InvertedKey key2 = (InvertedKey) o2;
			return key1.compareTo(key2);
	}
}
