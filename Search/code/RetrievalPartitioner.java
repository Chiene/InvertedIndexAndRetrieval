package Retrieval;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class RetrievalPartitioner extends Partitioner<RankKey,Text> {
	@Override
	public int getPartition(RankKey key,Text text, int numReduceTasks) {
			// customize which <K ,V> will go to which reducer
		String str_key = key.getWord();
		if(str_key.charAt(0)>='a'&&str_key.charAt(0)<='g')
			return 0;
		else return 1;
	}
}
