import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class KafkaProducerPlaneDate {
    public static void main(String[] args) {
        Properties properties = new Properties();

        // kafka bootstrap server
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        //properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        // producer acks
        properties.setProperty("acks", "1");
        properties.setProperty("retries", "3");
        properties.setProperty("linger.ms", "1");

        Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(properties);

        String fileName = "/Users/vishnoiprem/interview/company/5-emirates/data/plane-data.csv";
        String line = null;
        Integer key=1;

        try {
            // FileReader reads text files in the default encoding.
            FileReader fileReader = new FileReader(fileName);

            // Always wrap FileReader in BufferedReader.
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            while((line = bufferedReader.readLine()) != null) {
                //key1=line.split(",")[0];
                ProducerRecord<String, String> producerRecord =
                new ProducerRecord<String, String>("planedate", Integer.toString(key), "" + (line));
                producer.send(producerRecord);
                key++;
            }

            // Always close files.
            bufferedReader.close();
        }
        catch(FileNotFoundException ex) {
            System.out.println(
                    "Unable to open file '" +
                            fileName + "'");
        }
        catch(IOException ex) {
            System.out.println(
                    "Error reading file '"
                            + fileName + "'");
            // Or we could just do this:
            // ex.printStackTrace();
        }



        /*for (int key=0; key < 10; key++){
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<String, String>("Prem", Integer.toString(key), "message that has key: " + Integer.toString(key));
            producer.send(producerRecord);
        }*/



        producer.close();
    }
}
