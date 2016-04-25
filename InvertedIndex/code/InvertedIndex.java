package InvertedIndex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;

public class InvertedIndex {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs= FileSystem.get(conf);
		//get the FileStatus list from given dir
		FileStatus[] status_list = fs.listStatus(new Path(args[0]));
		if(status_list != null){
				int i=1;
				for(FileStatus status : status_list){
						//add each file to the list of inputs for the map-reduce job
						conf.set(status.getPath().getName(),String.valueOf(i));
						i++;
				}
		}

		Job job = Job.getInstance(conf, "InvertedIndex");
		job.setJarByClass(InvertedIndex.class);

		// set the class of each stage in mapreduce
		job.setMapperClass(InvertedIndexMapper.class);
		job.setPartitionerClass(InvertedIndexPartitioner.class);
		job.setCombinerClass(InvertedIndexCombiner.class);
		job.setSortComparatorClass(InvertedIndexComparator.class);
		job.setGroupingComparatorClass(InvertedIndexGroupComparator.class);
		job.setReducerClass(InvertedIndexReducer.class);

		// set the output class of Mapper and Reducer
		job.setMapOutputKeyClass(InvertedKey.class);
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
