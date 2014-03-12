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

public class ReviewsApestaMapperTest {

	private FileReader fileReader;
	private BufferedReader reader;

	@Before
	public void setUp() throws FileNotFoundException {
		String reviewsFilePath = ReviewsMCMapperTest.class.getResource(
				"/reviews-test.log").getPath();
		fileReader = new FileReader(reviewsFilePath);
		reader = new BufferedReader(fileReader);
	}

	@After
	public void tearDown() throws IOException {
		this.reader.close();
		this.fileReader.close();
	}
	
	@Test
	public void readLineForOtherMapperMap() throws IOException, InterruptedException {
		// arrange		
		String currentLine = reader.readLine();
		
		ReviewsApestaMapper reviewsMapper = new ReviewsApestaMapper();
		LongWritable key = new LongWritable();
		Text text = new Text(currentLine);
		Context context = Mockito.mock(Context.class);
		Mockito.doThrow(Exception.class).when(context).write(Mockito.any(Text.class), Mockito.any(Text.class));
		
		// act
		reviewsMapper.map(key, text, context);
		
		// assert
	}

	@Test
	public void readLineTest() throws IOException, InterruptedException {
		// arrange
		String expected = "{\"id\":2,\"sentiment\":\"Negative\",\"reviews\":[\"En enero pasado compre un teléfono celular para usarlo en el plan de libertad total de IUSACELL, que me inscribí esto lo hice en local que tienen en Plaza Tepeyac, resulta que el día que lo compré me entregaron el cel, pero sin servicio de telefonía y que\",\"Banamex me está aplicando comisiones por manejo de cuenta que no proceden, desde el mes de septiembre y hasta ahora solo me han regresado la de septiembre.El servicio de audiomático es lento, no resuelven el problema y parece que tienen la consigna de d\",\"ServiciosPowerPC ofrece COMPUTADORA HP EN PROMO CON 1AÑO DE GARANTIA EN $4000El proceso del fraude es:1. Contactan a conocidos tuyos por correo electrónico para ofrecer el equipo.2. También por teléfono celular y correo electrónico, proporcionan car\",\"Trabajo muy cerca del mbus Campeche y por mis actividades constantemente necesito cosas de ferretería las cuales hasta hoy compraba en la FERRETERÍA LA ESTRELLA UBICADA EN AV. INSURGENTES SUR NO. 3xx LOCAL 1, es un local pequeño ubicado junto a una cafete\",\"en tv azteca nos venden la idea de ser una \\\"señal con valor\\\", sin embargo, en la practica, esto es totalmente diferente. cada año esta empresa realiza el famoso \\\"jugueton\\\" donde se supone que se trata de una labor humanitaria y caritativa. yo no estoy e\",\"Comparto la terrible experiencia que vivi al ser victima de un fraude con mi tarjeta de credito Azul de Bancomer,esperando sirva para prevenir mas fraudes,intente hacer una compra mi tdc bancomer y fue denegado el cargo, llame a linea bancomer y dijeron q\",\"El pasado 28 febrero 14 y 03 marzo 14 se efectuaron el pago por concepto de renta de cimbra hablamos con señor que se acredito como dueñoEfectuando pago vía depósito bancario a nombre De un tercero que nos dieron el datoEn el banco Banamex por cas\",\"Quiero manifestar mi inconformidad con relación a la validación de una garantía en la Boutique Santa Fe de Swarovski , el día 26 de noviembre de 2013 llevé a garantía un anillo que se le cayó un cristal, los últimos días de diciembre del mismo año me llam\",\"Espero que mi experiencia les sirva aquellos que como yo siempre buscan ahorrar unos pesos. Despues de que me robaran mi celular unos policias, me dispuse acudir a eje central (AUN en contra de lo mal que hablan de EL) en busqueda de un celular que no pas\",\"El día 6 de enero de 2014 realice una compra a MEQUEDOUNO de varios artículos recibí un correo de el 23 de enero informándome que uno de los artículos no lo iban a enviar por un fallo en su proceso logístico y me dieron dos opcion\"]}";
		reader.readLine();
		reader.readLine();
		reader.readLine();
		String currentLine = reader.readLine();

		ArgumentCaptor<Text> reviewKeyCaptor = ArgumentCaptor
				.forClass(Text.class);
		ArgumentCaptor<Text> reviewBodyCaptor = ArgumentCaptor
				.forClass(Text.class);
		ReviewsApestaMapper reviewsApestaMapper = new ReviewsApestaMapper();
		LongWritable key = new LongWritable();
		Text text = new Text(currentLine);
		Context context = Mockito.mock(Context.class);
		Mockito.doNothing().when(context)
				.write(reviewKeyCaptor.capture(), reviewBodyCaptor.capture());

		// act
		reviewsApestaMapper.map(key, text, context);
		String actual = reviewBodyCaptor.getValue().toString();

		// assert
		assertEquals(expected, actual);
		assertEquals(Sentiment.Negative.toString(), reviewKeyCaptor.getValue()
				.toString());
	}

}
