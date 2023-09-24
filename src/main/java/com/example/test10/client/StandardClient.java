package com.example.test10.client;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.transaction.Transaction;

import com.example.test10.Config;
import com.example.test10.model.TestMessage;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StandardClient {

    private Transaction transaction;

    public static void main(String[] args) throws PulsarClientException {
        StandardClient client = new StandardClient();
        client.go();
    }

    @SuppressWarnings("java:S2189")
    private void go() throws PulsarClientException {
        PulsarClient client = PulsarClient.builder()
            .enableTransaction(true)
            .serviceUrl(Config.SERVICE_URL)
            .build();

        Consumer<TestMessage> inputConsumer = client
            .newConsumer(Schema.AVRO(TestMessage.class))
            .subscriptionName(Config.IN_SUB)
            .topic(Config.IN_TOPIC)
            .subscribe();
        while (true) {
            try (
                Producer<TestMessage> outputProducer1 = client
                    .newProducer(Schema.AVRO(TestMessage.class))
                    .topic(Config.OUT_TOPIC_1)
                    .create();
                Producer<TestMessage> outputProducer2 = client
                    .newProducer(Schema.AVRO(TestMessage.class))
                    .topic(Config.OUT_TOPIC_2)
                    .create();) {
                Message<TestMessage> message = inputConsumer.receive();
                if (transaction != null) {
                    abortTransaction(inputConsumer, message);
                }

                openTransaction(client);
                System.out.println("Received from: " + inputConsumer.getTopic());
                outputProducer1.send(message.getValue());
                System.out.println("Sending to:" + outputProducer1.getTopic());
                outputProducer2.send(message.getValue());
                System.out.println("Sending to:" + outputProducer2.getTopic());
                delay();
                ackAndCommitTransaction(inputConsumer, message);
            }
        }
    }

    private void delay() {
        long delay = 1100;
        System.out.println("Force delay for " + delay + " ms");
        try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void openTransaction(PulsarClient client) throws PulsarClientException {
        CompletableFuture<Transaction> transactionFuture = client.newTransaction().withTransactionTimeout(1, TimeUnit.SECONDS).build();
        try {
            transaction = transactionFuture.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            log.error("Cannot open transation", e);
        }
    }

    private void ackAndCommitTransaction(Consumer<TestMessage> consumer, Message<TestMessage> message) throws PulsarClientException {
        try {
            consumer.acknowledge(message);
            transaction.commit().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            log.error("Cannot commit transation", e);
            consumer.negativeAcknowledge(message);
        }
    }

    private void abortTransaction(Consumer<TestMessage> consumer, Message<TestMessage> message) {
        try {
            transaction.abort().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            consumer.negativeAcknowledge(message);
        }
    }
}
