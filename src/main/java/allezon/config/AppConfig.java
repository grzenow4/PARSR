package allezon.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import allezon.domain.AggregatedValue;
import allezon.domain.UserTagEvent;

@Configuration
public class AppConfig {

    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        return mapper;
    }

    @Bean
    public JsonSerde<UserTagEvent> userTagEventSerde() {
        return new JsonSerde<>(UserTagEvent.class);
    }

    @Bean
    public JsonSerde<AggregatedValue> aggregatedValueSerde() {
        return new JsonSerde<>(AggregatedValue.class);
    }
}
