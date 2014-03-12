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

public class ReviewsMCMapper extends Mapper<LongWritable, Text, Text, Text> {

	public static final String IGNORE = "ignore";

	@Override
	public void map(LongWritable key, Text text, Context context)
			throws IOException, InterruptedException {
		String htmlContent = text.toString();
		String[] htmlSplited = htmlContent.split(";");
		
		if(htmlSplited.length > 1){
			Reviews reviews = new Reviews();		
			long id = extractId(htmlSplited[0]);
			reviews.setId(id);

			List<String> reviewList = extractReviews(htmlSplited);
			for (String review : reviewList) {
				reviews.addReview(review);
			}
			
			Sentiment sentiment = extractSentiment(htmlSplited[1]);
			reviews.setSentiment(sentiment);
			
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

	private List<String> extractReviews(String[] htmlSplited) {
		List<String> reviewsList = new ArrayList<String>();
		String safeHTML = Joiner.on(";").join(htmlSplited);
		Iterable<String> reviewsRaw = Splitter.on("box_texto\">\"").trimResults().omitEmptyStrings().split(safeHTML);
		boolean ignoreFirts = false;
		for (String reviewRaw : reviewsRaw) {
			if(ignoreFirts){
				reviewsList.add(reviewRaw.split("\"")[0].trim());
			}else{
				ignoreFirts = true;
			}
		}
		return reviewsList;
	}

	private Sentiment extractSentiment(String htmlWithSentiment) {	
		htmlWithSentiment = htmlWithSentiment.trim().toLowerCase();
		Sentiment sentiment = Sentiment.Negative;
		switch (htmlWithSentiment.substring(0, 3)) {
		case "pos":
			sentiment = Sentiment.Positive;
			break;
		case "neg":
			sentiment = Sentiment.Negative;
			break;
		case "neu":
			sentiment = Sentiment.Neutral;
			break;
		}
		return sentiment;
	}

	private long extractId(String htmlWithId) {
		String[] htmlWithIdSplited = htmlWithId.split("-");
		String idString = htmlWithIdSplited[htmlWithIdSplited.length - 1].trim();
		if(!Strings.isNullOrEmpty(idString)){
			return Long.parseLong(idString);
		}
		return -1;
	}

}
