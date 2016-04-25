package InvertedIndex;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedIndexMapper extends Mapper<LongWritable, Text, InvertedKey, Text> {
	private Text tf = new Text("1");
	private InvertedKey invertedKey= new InvertedKey();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();
		String documentFileName = conf.get(((FileSplit)context.getInputSplit()).getPath().getName());
		FileSplit fileSplit = ((FileSplit)context.getInputSplit());
		//String documentFileName = fileSplit.getPath().getName();
		String delimiters = " |$&-!?[]/\t;:,.()\'    ";
		String regular_expression = "[a-zA-Z]+";

		StringTokenizer itr = new StringTokenizer(value.toString(),delimiters);
		int currentIndex = 0;
		while (itr.hasMoreTokens()) {
			String toProcess = itr.nextToken();
			if(toProcess.matches(regular_expression)) {
					currentIndex = value.toString().indexOf(toProcess,currentIndex);
					invertedKey.setWord(toProcess);
					invertedKey.setDocId(documentFileName);
					tf.set("1"+" "+String.valueOf(key.get()+currentIndex));
					context.write(invertedKey,tf);
					currentIndex+=toProcess.length()+1;
			}
		}
	}
}
