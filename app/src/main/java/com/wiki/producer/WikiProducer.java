package com.wiki.producer;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.sse.EventSource;
import okhttp3.sse.EventSourceListener;
import okhttp3.sse.EventSources;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class WikiProducer {
    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        CountDownLatch latch = new CountDownLatch(1);

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        OkHttpClient client = new OkHttpClient();

        Request request = new Request.Builder()
                .url("https://stream.wikimedia.org/v2/stream/recentchange")
                .addHeader("User-Agent", "wiki-stream-pipeline/1.0 (jackkellyboyd@gmail.com)")
                .build();

        EventSourceListener listener = new EventSourceListener() {
            @Override
            public void onEvent(@NonNull EventSource eventSource, @Nullable String id, @Nullable String type, @NonNull String data) {
                ProducerRecord<String, String> record = new ProducerRecord<>("wiki.recentchange", id, data);
                producer.send(record);
                System.out.println(data);
            }

            @Override
            public void onFailure(@NonNull EventSource eventSource, @Nullable Throwable t, @Nullable Response response) {
                System.out.println("Stream failed: " + t);
                System.out.println("Response: " + response);
                latch.countDown();
            }

            @Override
            public void onClosed(@NonNull EventSource eventSource) {
                latch.countDown();
            }
        };

        EventSources.createFactory(client).newEventSource(request, listener);
        latch.await();
    }
}