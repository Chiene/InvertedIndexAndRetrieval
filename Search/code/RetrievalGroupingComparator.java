package Retrieval;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class RetrievalGroupingComparator extends WritableComparator {

        public RetrievalGroupingComparator() {
                super(RankKey.class, true);
        }

        @Override
        public int compare(WritableComparable o1, WritableComparable o2) {
        		RankKey key1 = (RankKey) o1;
            RankKey key2 = (RankKey) o2;
            return 0;
        }
}
