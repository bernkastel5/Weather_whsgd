import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class WeatherConsumer {
    private static final String TOPIC = "weather-topic";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final Gson GSON = new Gson();

    private final Map<String, Map<LocalDate, WeatherData>> dataStore = new HashMap<>();
    private int messageCount = 0;
    private static final int MAX_MESSAGES = 20;

    public void consume() {
        System.out.println("Consumer started");
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "weather-group-" + System.currentTimeMillis());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Arrays.asList(TOPIC));
            while (messageCount < MAX_MESSAGES) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        WeatherData data = GSON.fromJson(record.value(), WeatherData.class);
                        dataStore
                                .computeIfAbsent(data.getCity(), k -> new HashMap<>())
                                .put(data.getLocalDate(), data);
                        messageCount++;
                        System.out.println("Received: " + record.value());
                        if (messageCount % 10 == 0) {
                            computeAndPrintAnalytics();
                        }
                    } catch (Exception e) {
                        System.out.println("Ошибка при разборе сообщения: " + record.value());
                        e.printStackTrace();
                    }
                }
            }
            computeAndPrintAnalytics();
        }
    }

    private void computeAndPrintAnalytics() {
        LocalDate today = LocalDate.now();
        LocalDate weekAgo = today.minusDays(7);

        String maxRainyCity = null;
        int maxRainyDays = 0;
        int maxTemp = -1;
        String maxTempCity = null;
        String maxTempDate = null;
        String minAvgTempCity = null;
        double minAvgTemp = Double.MAX_VALUE;

        for (Map.Entry<String, Map<LocalDate, WeatherData>> cityEntry : dataStore.entrySet()) {
            String city = cityEntry.getKey();
            Map<LocalDate, WeatherData> cityData = cityEntry.getValue();

            int rainyDays = 0;
            double totalTemp = 0;
            int dayCount = 0;

            for (Map.Entry<LocalDate, WeatherData> dateEntry : cityData.entrySet()) {
                LocalDate date = dateEntry.getKey();
                if (!date.isBefore(weekAgo) && !date.isAfter(today)) {
                    WeatherData wd = dateEntry.getValue();
                    if ("rainy".equals(wd.getCondition())) {
                        rainyDays++;
                    }
                    totalTemp += wd.getTemperature();
                    dayCount++;
                    if (wd.getTemperature() > maxTemp) {
                        maxTemp = wd.getTemperature();
                        maxTempCity = city;
                        maxTempDate = wd.getDate();
                    }
                }
            }

            if (rainyDays > maxRainyDays) {
                maxRainyDays = rainyDays;
                maxRainyCity = city;
            }

            if (dayCount > 0) {
                double avgTemp = totalTemp / dayCount;
                if (avgTemp < minAvgTemp) {
                    minAvgTemp = avgTemp;
                    minAvgTempCity = city;
                }
            }
        }

        System.out.println("Analytics:");
        System.out.println("City with max rainy days: " + maxRainyCity + " (" + maxRainyDays + " days)");
        System.out.println("Hottest weather: " + maxTemp + "°C on " + maxTempDate + " in " + maxTempCity);
        System.out.println("City with lowest avg temperature: " + minAvgTempCity + " (" + minAvgTemp + "°C)");
        System.out.println("---------------------");
    }
}
