package Retrieval;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class RetrievalComparator extends WritableComparator {

	public RetrievalComparator() {
		super(RankKey.class, true);
	}

	@Override
	public int compare(WritableComparable o1, WritableComparable o2) {
		RankKey key1 = (RankKey) o1;
		RankKey key2 = (RankKey) o2;
		float key1Score = Float.valueOf(key1.getScore());
		float key2Score = Float.valueOf(key2.getScore());
		if(key1Score > key2Score) {
			return -1;
		} else if(key1Score < key2Score) {
			return 1;
		} else {
			return 0;
		}
	}
}
