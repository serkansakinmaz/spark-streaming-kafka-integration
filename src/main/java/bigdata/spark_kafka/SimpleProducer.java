package bigdata.spark_kafka;

import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.google.gson.Gson;

/**
 * @author serkansakinmaz
 */
public class SimpleProducer {

	private static Scanner in;
	private static Gson gson;
	private static final String TOPIC_NAME;

	static {
		gson = new Gson();
		TOPIC_NAME = "ecommerce_topic";
	}

	public static void main(String[] args) {
		in = new Scanner(System.in);
		System.out.println("Enter productName --> (type exit to quit)\n");

		Producer<String, String> producer = new KafkaProducer<String, String>(createKafkaProperties());

		String line = in.nextLine();
		while (!line.equals("exit")) {
			EcommerceLog contact = new EcommerceLog(line);
			String json = gson.toJson(contact);
			System.out.println("Sending kafka : " + json);
			ProducerRecord<String, String> rec = new ProducerRecord<String, String>(TOPIC_NAME, json);
			producer.send(rec);
			line = in.nextLine();
		}
		in.close();
		producer.close();

	}

	private static Properties createKafkaProperties() {
		Properties configProperties = new Properties();
		configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");
		configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");
		return configProperties;
	}
}
