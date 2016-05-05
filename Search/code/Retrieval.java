package Retrieval;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;

public class Retrieval {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		FileSystem fs= FileSystem.get(conf);
		//get the FileStatus list from given dir
		FileStatus[] status_list = fs.listStatus(new Path(args[2]));
		conf.set("FileInputPath",args[2]);
		conf.set("parameter",args[3]);
		conf.set("docSize",String.valueOf(status_list.length));
		Job job = Job.getInstance(conf, "Retrieval");
		job.setJarByClass(Retrieval.class);


		// set the class of each stage in mapreduce
		job.setMapperClass(RetrievalMapper.class);
		job.setPartitionerClass(RetrievalPartitioner.class);
		job.setSortComparatorClass(RetrievalComparator.class);
		job.setGroupingComparatorClass(RetrievalGroupingComparator.class);
		job.setReducerClass(RetrievalReducer.class);

		// set the output class of Mapper and Reducer
		job.setMapOutputKeyClass(RankKey.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// set the number of reducer
		job.setNumReduceTasks(1);

		// add input/output path
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
