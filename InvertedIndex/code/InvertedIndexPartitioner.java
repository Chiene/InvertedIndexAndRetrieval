package InvertedIndex;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class InvertedIndexPartitioner extends Partitioner<InvertedKey,Text > {
	@Override
	public int getPartition(InvertedKey key, Text value, int numReduceTasks) {
		// customize which <K ,V> will go to which reducer
		String str_key = key.getWord();
		if(str_key.charAt(0)>='a'&&str_key.charAt(0)<='z')
			return 0;
		else
			return 1;
	}
}
