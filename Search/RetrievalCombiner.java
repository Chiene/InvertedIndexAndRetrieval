package Retrieval;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RetrievalCombiner extends Reducer<Text,SumCountPair,Text,SumCountPair> {
	// Combiner implements method in Reducer

	SumCountPair sumCountPair = new SumCountPair();
	Text text = new Text();
    public void reduce(Text key, Iterable<SumCountPair> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		int count = 0;
		for (SumCountPair val: values) {
			sum += val.getSum();
			count += val.getCount();
		}
		text.set(key);
		sumCountPair.setCount(count);
		sumCountPair.setSum(sum);
		context.write(text,sumCountPair);

	}
}
