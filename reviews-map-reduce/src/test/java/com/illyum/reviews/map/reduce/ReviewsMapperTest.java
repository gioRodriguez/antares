package com.illyum.reviews.map.reduce;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class ReviewsMapperTest {	
	
	private FileReader fileReader;
	private BufferedReader reader;
	
	@Before
	public void setUp() throws FileNotFoundException{
		String reviewsFilePath = ReviewsMapperTest.class.getResource("/reviews-test.log").getPath();		
		fileReader = new FileReader(reviewsFilePath);
		reader = new BufferedReader(fileReader);
	}
	
	@After
	public void tearDown() throws IOException{
		this.reader.close();
		this.fileReader.close();
	}

	@Test
	public void postivesMap() throws IOException, InterruptedException {
		// arrange
		String expected = "{\"id\":130106125,\"sentiment\":\"Positive\",\"reviews\":[\"muy buena\",\"todo bien\",\"¡Excelente vendedor!\",\"EXCELENTE VENDEDOR 100% RECOMENDABLE\",\"La atención fue buena y rapida\",\"Muy buen vendedor, muy atento. La silla esta tal cual en la foto. recomendable\",\"El trato y atención excelentes. Lo único fue que el producto no trae descripción de ingredientes.\"]}";		
		String currentLine = reader.readLine();
		
		ArgumentCaptor<Text> reviewKeyCaptor = ArgumentCaptor.forClass(Text.class);
		ArgumentCaptor<Text> reviewBodyCaptor = ArgumentCaptor.forClass(Text.class);
		ReviewsMapper reviewsMapper = new ReviewsMapper();
		LongWritable key = new LongWritable();
		Text text = new Text(currentLine);
		Context context = Mockito.mock(Context.class);
		Mockito.doNothing().when(context).write(reviewKeyCaptor.capture(), reviewBodyCaptor.capture());
		
		// act
		reviewsMapper.map(key, text, context);
		String actual = reviewBodyCaptor.getValue().toString();
		
		// assert
		assertEquals(expected, actual);
		assertEquals(Sentiment.Positive.toString(), reviewKeyCaptor.getValue().toString());
	}
	
	@Test
	public void negativesMap() throws IOException, InterruptedException {
		// arrange
		String expected = "{\"id\":130106125,\"sentiment\":\"Negative\",\"reviews\":[]}";
		reader.readLine();
		String currentLine = reader.readLine();
		
		ArgumentCaptor<Text> reviewKeyCaptor = ArgumentCaptor.forClass(Text.class);
		ArgumentCaptor<Text> reviewBodyCaptor = ArgumentCaptor.forClass(Text.class);
		ReviewsMapper reviewsMapper = new ReviewsMapper();
		LongWritable key = new LongWritable();
		Text text = new Text(currentLine);
		Context context = Mockito.mock(Context.class);
		Mockito.doNothing().when(context).write(reviewKeyCaptor.capture(), reviewBodyCaptor.capture());
		
		// act
		reviewsMapper.map(key, text, context);
		String actual = reviewBodyCaptor.getValue().toString();
		
		// assert
		assertEquals(expected, actual);
		assertEquals(ReviewsMapper.IGNORE, reviewKeyCaptor.getValue().toString());
	}
	
	@Test
	public void neutralMap() throws IOException, InterruptedException {
		// arrange
		String expected = "{\"id\":130106125,\"sentiment\":\"Neutral\",\"reviews\":[\"Bueno\"]}";
		reader.readLine();
		reader.readLine();
		String currentLine = reader.readLine();
		
		ArgumentCaptor<Text> reviewKeyCaptor = ArgumentCaptor.forClass(Text.class);
		ArgumentCaptor<Text> reviewBodyCaptor = ArgumentCaptor.forClass(Text.class);
		ReviewsMapper reviewsMapper = new ReviewsMapper();
		LongWritable key = new LongWritable();
		Text text = new Text(currentLine);
		Context context = Mockito.mock(Context.class);
		Mockito.doNothing().when(context).write(reviewKeyCaptor.capture(), reviewBodyCaptor.capture());
		
		// act
		reviewsMapper.map(key, text, context);
		String actual = reviewBodyCaptor.getValue().toString();
		
		// assert
		assertEquals(expected, actual);
		assertEquals(Sentiment.Neutral.toString(), reviewKeyCaptor.getValue().toString());
	}

}
