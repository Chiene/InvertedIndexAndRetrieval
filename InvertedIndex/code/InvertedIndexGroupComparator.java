package InvertedIndex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class InvertedIndexGroupComparator extends WritableComparator {
    private Text word1 = new Text();
  	private Text word2 = new Text();

  	public InvertedIndexGroupComparator() {
  		super(InvertedKey.class, true);
  	}


  	public int compare(WritableComparable o1, WritableComparable o2) {
        InvertedKey key1 = (InvertedKey) o1;
        InvertedKey key2 = (InvertedKey) o2;
        return key1.compareToKey(key2);
  	}

}
