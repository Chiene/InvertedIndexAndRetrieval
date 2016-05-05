package Retrieval;
import java.util.Arrays;
import java.io.IOException;
import java.io.DataInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.SequenceFile;
public class RetrievalReducer extends Reducer<RankKey,Text,Text,Text> {

    private Text text = new Text();
    private Text value = new Text();

    public void reduce(RankKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    FileSystem fs= FileSystem.get(conf);
		//get the FileStatus list from given dir
		FileStatus[] status_list = fs.listStatus(new Path(conf.get("FileInputPath")));
    //conf.set(status.getPath().getName(),String.valueOf(i));

    int count = 0;
		// agrregate the amount of same starting character
    int i = 1;
    int rankNumber = 1;
    float lastScore = 0;
    for (Text val: values) {
      if(i > 10) break;
      String items[] = val.toString().split(" ");
      String offsetsStr[] = (items[1].substring(1,items[1].length()-1)).split(",");
      int offsets[] = new int[offsetsStr.length];
      for(int j = 0;j<offsetsStr.length;j++) {
        offsets[j] = Integer.valueOf(offsetsStr[j]);
      }
      String positings = "";
      Arrays.sort(offsets);
      if(Float.valueOf(key.getScore()) > 0) {
        if(lastScore == Float.valueOf(key.getScore())) {

        } else {
          rankNumber = i;
        }
        lastScore = Float.valueOf(key.getScore());

        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status_list[Integer.valueOf(items[0])-1].getPath())));

        int offset = 0;
        for(int off : offsets) {
          String temp ="";
          while(off > offset) {
            temp = br.readLine();
            if(temp != null) {
              offset += temp.getBytes().length+1;
              if(off < offset) {
                positings  += String.format("%d\t%s\n",off,temp);
                break;
              }
            } else {
              offset++;
            }
          }
        }
        br.close();

        text.set(key.getWord()+"\n"+getKey(rankNumber,status_list[Integer.valueOf(items[0])-1].getPath().getName(),key.getScore())+"\n");
        context.write(text,new Text("\n" + positings));

      }
      i+=1;
    }

		// write the result
		// context.write(K,V);

	}

  public String getKey(int i,String index,String score) {
      return String.format("Rank %d: %s socre: %s",i,index,score);
  }



}
