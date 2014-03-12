package com.illyum.reviews.map.reduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReviewsReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> reviewValues, Context context)
			throws IOException, InterruptedException {
		for (Text text : reviewValues) {
			context.write(key, text);
		}
	}
}
