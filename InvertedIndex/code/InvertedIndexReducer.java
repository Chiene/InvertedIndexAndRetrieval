package InvertedIndex;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReducer extends Reducer<InvertedKey,Text,Text,Text> {

    private Text value = new Text();
    public void reduce(InvertedKey key, Iterable<Text> iterValues, Context context) throws IOException, InterruptedException {

      int df = 0;
      String contentValue ="";
      // agrregate the amount of same starting character
      for (Text val: iterValues) {
        df+=1;
        contentValue += val.toString()+";";
      }
      value.set(String.format("%d:%s",df,contentValue));

      context.write(new Text(key.getWord()),value);
	}
}
