package Retrieval;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class RankKey implements WritableComparable  {
        private String word;
        private String score;

        public RankKey() {

        }

				public void setScore(String _score) {
					score = _score;
				}

				public void setWord(String _word) {
					word = _word;
				}

        public String getWord() {
                return word;
        }

        public String getScore() {
                return score;
        }
        @Override
        public void write(DataOutput out) throws IOException {
                out.writeUTF(word);
                out.writeUTF(score);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
                word = in.readUTF();
								score = in.readUTF();
				}

        @Override
        public int compareTo(Object in) {
						return 0;
        }
}
