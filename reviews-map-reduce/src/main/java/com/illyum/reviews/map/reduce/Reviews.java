package com.illyum.reviews.map.reduce;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Reviews implements Serializable{
	private static final long serialVersionUID = 1L;
	
	private long id;
	private Sentiment sentiment;
	private List<String> reviews;
	
	public Reviews() {
		this.reviews = new ArrayList<String>();
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public Sentiment getSentiment() {
		return sentiment;
	}

	public void setSentiment(Sentiment sentiment) {
		this.sentiment = sentiment;
	}

	public List<String> getReviews() {
		return reviews;
	}

	public void addReview(String review) {
		this.reviews.add(review);
	}
}
