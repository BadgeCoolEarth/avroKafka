package org.apache.client;

import java.io.IOException;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;

public class AvroProducerTest {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		
		String topic = "topic10";
		if (args.length >= 1) {
			topic =args[0];
		}
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.20.8.14:9092");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
		          io.confluent.kafka.serializers.KafkaAvroSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
		          io.confluent.kafka.serializers.KafkaAvroSerializer.class);
		props.put("schema.registry.url", "http://10.20.8.14:8081");
		KafkaProducer producer = new KafkaProducer(props);
	
		String key = "key8";
		String userSchema = "{\"type\":\"record\"," +
		                    "\"name\":\"myrecord\"," +
		                    "\"fields\":[{\"name\":\"sky\",\"type\":\"string\"}]}";
		Schema.Parser parser = new Schema.Parser();
		Schema schema = parser.parse(userSchema);
		GenericRecord avroRecord = new GenericData.Record(schema);
		avroRecord.put("sky", "value6");
		
		/*ByteArrayOutputStream out = new ByteArrayOutputStream();
		DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(avroRecord.getSchema());
		Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
		writer.write(avroRecord, encoder);
		encoder.flush();
		
		out.close();
		
		byte[] msg = out.toByteArray();*/
		
		ProducerRecord<String, GenericRecord> record = new ProducerRecord<String, GenericRecord>(topic, key, avroRecord);
		//ProducerRecord<String, byte[]> record = new ProducerRecord<String, byte[]>(topic, key, msg);
		try {
		  producer.send(record);
		  producer.close();		//用完连接之后一定要关闭
		} catch(SerializationException e) {
		  // may need to do something with it
			e.printStackTrace();
			System.out.println(e.getMessage());
		}
	}

}
