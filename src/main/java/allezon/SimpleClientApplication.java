package allezon;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams
public class SimpleClientApplication {

    private static final Logger log = LoggerFactory.getLogger(SimpleClientApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(SimpleClientApplication.class, args);
    }

}
