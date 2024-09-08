package allezon;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams
public class AllezonApp {
    public static void main(String[] args) {
        SpringApplication.run(AllezonApp.class, args);
    }

}
