package com.illyum.reviews.map.reduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.gson.Gson;

public class ReviewsApestaMapper extends Mapper<LongWritable, Text, Text, Text> {
	private static final String URL_APESTA = "GetUrlApestaPrototype";
	private static final String APESTA_REVIEWS_SEPARATOR = " :";
	public static final String IGNORE = "ignore";
	
	@Override
	public void map(LongWritable key, Text text, Context context)
			throws IOException, InterruptedException {
		String htmlContent = text.toString();
		String[] htmlSplited = htmlContent.split(APESTA_REVIEWS_SEPARATOR);
		
		if(htmlSplited.length > 1 && htmlSplited[0].contains(URL_APESTA)){
			Reviews reviews = new Reviews();		
			long id = extractId(htmlSplited[1]);
			reviews.setId(id);

			List<String> reviewList = extractReviews(htmlSplited);
			for (String review : reviewList) {
				reviews.addReview(review);
			}
			
			reviews.setSentiment(Sentiment.Negative);
			
			Gson gson = new Gson();
			Text reviewKey = new Text();
			Text reviewText = new Text();
			String reviewId = String.format("%s", reviews.getSentiment());
			if(reviews.getReviews().size() == 0){
				reviewId = IGNORE;
			}
			reviewKey.set(reviewId);		
			
			reviewText.set(gson.toJson(reviews));
			context.write(reviewKey, reviewText);
		}				
	}
	
	private long extractId(String htmlWithId) {
		String[] htmlWithIdSplited = htmlWithId.trim().split(" ");
		String idString = htmlWithIdSplited[0].trim();
		if(!Strings.isNullOrEmpty(idString)){
			return Long.parseLong(idString);
		}
		return -1;
	}
	
	private List<String> extractReviews(String[] htmlSplited) {
		List<String> reviewsList = new ArrayList<String>();
		String safeHTML = Joiner.on(";").join(htmlSplited);
		Iterable<String> reviewsRaw = Splitter.on("<div class=\"blueField white\">").trimResults().omitEmptyStrings().split(safeHTML);
		boolean ignoreFirts = false;
		for (String reviewRaw : reviewsRaw) {
			if(ignoreFirts){
				String reviewBody = reviewRaw.split("</div>")[0].trim();
				while(reviewBody.contains("\u003ca class")){
					int beginIgnore = reviewBody.indexOf("\u003ca class");
					int endIgnore = reviewBody.indexOf("script\u003e");
					String script = reviewBody.substring(beginIgnore - 1,  endIgnore + "script\u003e".length());
					reviewBody = reviewBody.replace(script, "");
				}
				reviewsList.add(reviewBody);
			}else{
				ignoreFirts = true;
			}
		}
		return reviewsList;
	}
}
