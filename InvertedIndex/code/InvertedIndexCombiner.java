	package InvertedIndex;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexCombiner extends Reducer<InvertedKey,Text,InvertedKey,Text> {
	// Combiner implements method in Reducer
		private Text text = new Text();
    public void reduce(InvertedKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		String locations = "";
		for(Text value:values) {
			String valueItems[] = value.toString().split(" ");
			sum+= Integer.valueOf(valueItems[0].toString());
			locations+=valueItems[1].toString()+",";
		}

		locations = locations.substring(0,locations.length()-1);
		text.set(String.format("%s,%d->[%s]", key.getDocId(), sum,locations));
		context.write(key,text);
	}
}
