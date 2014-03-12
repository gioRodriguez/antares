package com.illyum.reviews.map.reduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ReviewsMain {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJarByClass(ReviewsMain.class);
		
		job.setJobName("reviews job");
		
		job.setMapperClass(ReviewsMapper.class);
		job.setReducerClass(ReviewsReducer.class);
		job.setNumReduceTasks(1);
		
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
	
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}

}
