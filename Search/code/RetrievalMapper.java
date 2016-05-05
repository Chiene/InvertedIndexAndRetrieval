package Retrieval;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RetrievalMapper extends Mapper<LongWritable, Text, RankKey, Text> {

	private RankKey rankKey = new RankKey();
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// we simply use StringTokenizer to split the words for us.
		String keyword = context.getConfiguration().get("parameter");
		System.out.println("GetWord parameter: " + keyword);
		String keywordList[] = keyword.split(",");
		int docSize = Integer.valueOf(context.getConfiguration().get("docSize"));
		String line = value.toString();
		String word = line.substring(0,line.indexOf("\t"));
		System.out.println("GetWord: "+word);

		for(int j=0 ;j<keywordList.length;j++) {
			System.out.println("GetWord keyWord: " +keywordList[0]);
			if(keywordList[j].equalsIgnoreCase(word)){
					String val = line.substring(line.indexOf("\t")+1);
					String items[] = val.split(":|;");
					/*
					System.out.println("Split: "+val);
					for (int i = 0; i < items.length; i++)
					    System.out.println("Split items: "+items[i]);*/
					int df = Integer.valueOf(items[0]);
					System.out.println("Split df: "+df);
					for(int i=1;i<items.length;i++) {
						 String docItems[] = items[i].split("->");
						 String docInfo[] = docItems[0].split(",");
						 int tf = Integer.valueOf(docInfo[1]);
						 System.out.println(String.format("Split docid: %s, tf: %d",docInfo[0],tf));
						 float score =(float) tf*(float)Math.log10(docSize/df);
						 rankKey.setWord(word);
						 rankKey.setScore(String.valueOf(score));
						context.write(rankKey,new Text(String.format("%s %s",docInfo[0],docItems[1])));
					}
			}
		}
	}

}
