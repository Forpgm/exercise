package com.example.person_service.configuration;


import java.util.Map;

import com.example.person_service.dto.request.CreatePersonEvent;
import com.example.person_service.dto.request.ReadPersonEvent;
import com.example.person_service.dto.request.UpdatePersonEvent;
import com.example.person_service.entity.FailPersonEvent;
import com.example.person_service.service.FailPersonEventService;
import com.example.person_service.utils.PersonEventMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.ssl.DefaultSslBundleRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
@RequiredArgsConstructor
@Slf4j
@EnableKafka
@EnableConfigurationProperties(KafkaRetryProperties.class)
public class KafkaConfiguration {
    private final KafkaProperties kafkaProperties;
    private final KafkaRetryProperties kafkaRetryProperties;
    private final FailPersonEventService failPersonEventService;
    private final PersonEventMapper personEventMapper;
    private final String manualConsumptionTopic = "manual-consumption-topic";
    private final String group_id = "manual-consumption-group";

    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> configProps = kafkaProperties.buildConsumerProperties(new DefaultSslBundleRegistry());
        // JSON config
        configProps.put(JsonDeserializer.TRUSTED_PACKAGES, "com.netcompany.internal.training.exercise.dto.request");
        // consumer config
        configProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        configProps.put(ProducerConfig.RETRY_BACKOFF_MAX_MS_CONFIG, 5000);
        // Deserializer config
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        configProps.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        configProps.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(configProps);
    }

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> configProps = kafkaProperties.buildProducerProperties(new DefaultSslBundleRegistry());
        configProps.put(ProducerConfig.RETRIES_CONFIG, kafkaRetryProperties.attempts());
        configProps.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, kafkaRetryProperties.interval());
        configProps.put(ProducerConfig.ACKS_CONFIG, "all");
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, Object> kafkaTemplate) {
        DefaultErrorHandler errorHandler = getDefaultErrorHandler(kafkaTemplate);
        errorHandler.addNotRetryableExceptions(IllegalArgumentException.class, SerializationException.class);
        return errorHandler;
    }

    public DefaultErrorHandler getDefaultErrorHandler(KafkaTemplate<String, Object> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, exception) -> {
                    try {
                        log.info("Saved exceeded attempt event to database before DLT: eventId={}",
                                record.value());
                        FailPersonEvent failPersonEvent = null;
                        if (record.value() instanceof CreatePersonEvent) {
                            failPersonEvent =
                                    personEventMapper.extractCreatePersonEvent((CreatePersonEvent) record.value());
                        } else if (record.value() instanceof ReadPersonEvent) {
                            failPersonEvent =
                                    personEventMapper.extractReadPersonEvent((ReadPersonEvent) record.value());
                        } else if (record.value() instanceof UpdatePersonEvent) {
                            failPersonEvent =
                                    personEventMapper.extractUpdatePersonEvent((UpdatePersonEvent) record.value());
                        }
                        failPersonEventService.save(failPersonEvent);
                    } catch (Exception dbException) {
                        log.error("Failed to save to database, but continuing with DLT send", dbException);
                    }
                    String dltTopicName = record.topic() + "-dlt";
                    return new TopicPartition(dltTopicName, record.partition());
                });

        BackOff fixedBackOff = new FixedBackOff(kafkaRetryProperties.interval(), kafkaRetryProperties.attempts());
        return new DefaultErrorHandler(recoverer, fixedBackOff);
    }

    @Bean("batchConsumerFactory")
    public ConsumerFactory<String, Object> batchConsumerFactory() {
        Map<String, Object> configProps = kafkaProperties.buildConsumerProperties(new DefaultSslBundleRegistry());
        configProps.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        configProps.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        configProps.put(JsonDeserializer.TRUSTED_PACKAGES, "com.netcompany.internal.training.exercise.dto.request");
        // Batch config
        configProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        configProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 3);
        return new DefaultKafkaConsumerFactory<>(configProps);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(KafkaTemplate<String
            , Object> kafkaTemplate) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setBatchListener(false);
        factory.setCommonErrorHandler(errorHandler(kafkaTemplate));
        return factory;
    }

    @Bean("batchKafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaBatchListenerContainerFactory(KafkaTemplate<String, Object> kafkaTemplate) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(batchConsumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.setBatchListener(true);
        factory.setCommonErrorHandler(errorHandler(kafkaTemplate));
        return factory;
    }

    @Bean
    public NewTopic createManualConsumptionTopic() {
        return TopicBuilder.name(manualConsumptionTopic)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean("manualConsumerFactory")
    public ConsumerFactory<String, Object> manualConsumerFactory() {
        Map<String, Object> configProps = kafkaProperties.buildConsumerProperties(new DefaultSslBundleRegistry());
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, group_id);
        configProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        configProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        configProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);

        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        configProps.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        configProps.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        configProps.put(JsonDeserializer.TRUSTED_PACKAGES, "com.netcompany.internal.training.exercise.dto.request");
        return new DefaultKafkaConsumerFactory<>(configProps);
    }
}