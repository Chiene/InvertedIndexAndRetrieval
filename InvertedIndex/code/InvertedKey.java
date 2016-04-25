package InvertedIndex;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class InvertedKey implements WritableComparable  {
        private String word;
        private String docId;

        public InvertedKey() {

        }

				public void setDocId(String _docId) {
					docId = _docId;
				}

				public void setWord(String _word) {
					word = _word;
				}

        public String getWord() {
                return word;
        }

        public String getDocId() {
                return docId;
        }
        @Override
        public void write(DataOutput out) throws IOException {
                out.writeUTF(word);
                out.writeUTF(docId);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
                word = in.readUTF();
								docId = in.readUTF();
				}

        @Override
        public int compareTo(Object in) {
            InvertedKey target = (InvertedKey) in;
            int result = word.compareTo(target.getWord());
            if(result == 0) {
              int id = Integer.valueOf(docId);
              int targetId = Integer.valueOf(target.getDocId());
              if(id > targetId ) {
                result =1;
              }else if (id <targetId) {
                result = -1;
              }
              else {
                result = 0;
              }
            }
            return result;
        }

        public int compareToKey(Object in) {
          InvertedKey target = (InvertedKey) in;
          return word.compareTo(target.getWord());
        }
}
