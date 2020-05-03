package com.hcl.dna.ads.framework.process;

import com.hcl.dna.ads.framework.process.model.Customer;
import com.hcl.dna.ads.framework.process.model.Order;
import com.hcl.dna.ads.framework.process.model.OrderDetail;
import com.hcl.dna.ads.framework.process.model.Product;
import com.hcl.dna.ads.framework.process.util.ApplicationConstants;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class StreamProcessInitiator1 {

    public static void main(String[] args) {
        /*Map<String, String> props = new HashMap<>();
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");*/
        KafkaStreams streams = new KafkaStreams(
                topology(Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081")),
                streamsConfig("localhost:9092", 0, args[0], "localhost"));

        System.out.println("Stream is set to start !!!");
        streams.start();
        System.out.println("Stream is started !!!");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                streams.close();
            } catch (final Exception e) {
                System.out.println("Error");
            }
        }));
    }

    static Properties streamsConfig(final String bootstrapServers,
                                    final int applicationServerPort,
                                    final String stateDir,
                                    final String host) {
        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "web-click-charts");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfiguration.put(StreamsConfig.APPLICATION_SERVER_CONFIG, host + ":" + applicationServerPort);
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 500);
        String metadataMaxAgeMs = System.getProperty(ConsumerConfig.METADATA_MAX_AGE_CONFIG);

        if (metadataMaxAgeMs != null) {
            try {
                final int value = Integer.parseInt(metadataMaxAgeMs);
                streamsConfiguration.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, 100);
                System.out.println("Set consumer configuration " + ConsumerConfig.METADATA_MAX_AGE_CONFIG +
                        " to " + value);
            } catch (final NumberFormatException ignored) {
                ignored.printStackTrace();
            }
        }

        return streamsConfiguration;
    }

    public static Topology topology(final Map<String, String> serdeConfig) {

        Serde stringSerde = Serdes.String();

        Map<String, String> stringConfig = new HashMap<>();
        stringConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        Serde genericSerde = Serdes.String();
        genericSerde.configure(stringConfig, false);

        SpecificAvroSerde<Customer> customerSerde = new SpecificAvroSerde<>();
        customerSerde.configure(serdeConfig, false);

        SpecificAvroSerde<Product> productSerde = new SpecificAvroSerde<>();
        productSerde.configure(serdeConfig, false);

        SpecificAvroSerde<Order> orderSerde = new SpecificAvroSerde<>();
        orderSerde.configure(serdeConfig, false);

        SpecificAvroSerde<OrderDetail> orderDetailSerde = new SpecificAvroSerde<>();
        orderDetailSerde.configure(serdeConfig, false);

        StreamsBuilder builder = new StreamsBuilder();

        //pre populate from txt file: kafka-connect for future
        GlobalKTable<String, Customer> customerTable = builder.globalTable(ApplicationConstants.CUSTOMER_TOPIC,
                Consumed.with(stringSerde, customerSerde));

        //pre populate from txt file
        GlobalKTable<String, Product> productTable = builder.globalTable(ApplicationConstants.PRODUCT_TOPIC,
                Consumed.with(stringSerde, productSerde));

        KStream<String, String> productStream = builder.stream(ApplicationConstants.CURRENT_PRODUCT_TOPIC,
                Consumed.with(stringSerde, genericSerde));

        KStream<String, Product> detailedProductStream = productStream.join(
                productTable,
                (productStreamKey, productStreamValue) -> {
                    System.out.println("productStreamValue -> "+productStreamValue);
                    return productStreamValue;
                },
                (productStreamValue, productTableValue) ->  {
                    System.out.println("productTableValue.getName().toString() -> "+productTableValue.getName());
                    return productTableValue;
                }
        );

        //detailedProductStream.mapValues(product -> product).to("DUMMY", Produced.with(stringSerde, productSerde));

        KStream<String, Order> orderStream = builder.stream(ApplicationConstants.CART_TOPIC,
                Consumed.with(stringSerde, orderSerde));

        KStream<String, OrderDetail> detailsOfOrderStreamWithCustomer = orderStream.join(customerTable,
                (orderKey, customerKey) -> {
                    System.out.println("productStreamValue -> " + orderKey);
                    return orderKey;
                },
                (order, customer) -> {
                    System.out.println("Creating OrderDetail !!!");
                    CharSequence payment = order.getPayment() != null ? order.getPayment() : "";
                    CharSequence shipment = order.getShipment() != null ? order.getShipment() : "";
                    OrderDetail od =
                            OrderDetail.newBuilder().setId(order.getId()).setProductName(order.getProductId())
                                    .setCustomerName(String.join(", ", customer.getLastName(), customer.getFirstName()))
                                    .setStatus(order.getStatus()).setStatus(order.getStatus()).setPayment(payment)
                                    .setShipment(shipment).build();
                    System.out.println("Created OrderDetails with Order data and CustomerName !!!");
                    return od;
                }
        );

        KStream<String, OrderDetail> detailsOfOrderStreamWithCustomerProduct = detailsOfOrderStreamWithCustomer.join(
                detailedProductStream,
                (orderDetail, product) -> {
                    System.out.println("Updating OrderDetail with ProductName !!!");
                    CharSequence payment = orderDetail.getPayment() != null ? orderDetail.getPayment() : "";
                    CharSequence shipment = orderDetail.getShipment() != null ? orderDetail.getShipment() : "";
                    OrderDetail od =
                            OrderDetail.newBuilder().setId(orderDetail.getId()).setProductName(product.getName())
                                    .setCustomerName(orderDetail.getCustomerName()).setStatus(orderDetail.getStatus())
                                    .setPayment(payment).setShipment(shipment).build();
                    System.out.println("Updated OrderDetails with Product data !!!");
                    return od;
                },
                JoinWindows.of(600000l)
        );

        detailsOfOrderStreamWithCustomerProduct.mapValues(orderDetail -> orderDetail).to(ApplicationConstants.TRAGET_TOPIC,
                Produced.with(stringSerde, orderDetailSerde));
        return builder.build();
    }
}
