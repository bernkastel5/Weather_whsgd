import com.google.gson.Gson;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class WeatherProducer {
    private static final String TOPIC = "weather-topic";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final List<String> CITIES = Arrays.asList("Magadan", "Anadyr", "St. Petersburg", "Tyumen", "Moscow");
    private static final List<String> CONDITIONS = Arrays.asList("sunny", "cloudy", "rainy");
    private static final Random RANDOM = new Random();
    private static final Gson GSON = new Gson();

    public void produce() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < 20; i++) {
                WeatherData data = generateRandomWeather();
                String json = GSON.toJson(data);
                producer.send(new ProducerRecord<>(TOPIC, json));
                System.out.println("Sent: " + json);
                Thread.sleep(500);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private WeatherData generateRandomWeather() {
        WeatherData data = new WeatherData();
        data.setCity(CITIES.get(RANDOM.nextInt(CITIES.size())));
        LocalDate today = LocalDate.now();
        LocalDate randomDate = today.minusDays(RANDOM.nextInt(7));
        data.setDate(randomDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
        data.setTemperature(RANDOM.nextInt(36)); // 0-35
        data.setCondition(CONDITIONS.get(RANDOM.nextInt(CONDITIONS.size())));
        return data;
    }
}
