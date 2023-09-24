package com.example.test10;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;

import com.example.test10.model.TestMessage;
import com.example.test10.vp.FunctionUtils;
import com.example.test10.vp.MessageWrapper;
import com.example.test10.vp.Metadata;

import lombok.extern.slf4j.Slf4j;

@Slf4j
class InFunction implements Function<TestMessage, Void> {

    private Transaction transaction;

    @Override
    public Void process(TestMessage input, Context context) throws PulsarClientException {
        if (transaction != null) {
            abortTransaction(context);
        }

        System.out.println("Received from: " + context.getCurrentRecord().getTopicName());
        MessageWrapper<TestMessage> wrapper = new MessageWrapper<>(input, new Metadata());
        openTransaction(context);
        System.out.println("Sending to:" + Config.OUT_TOPIC_1);
        FunctionUtils.produce(wrapper, context, Config.OUT_TOPIC_1, TestMessage.class);
        System.out.println("Sending to:" + Config.OUT_TOPIC_2);
        FunctionUtils.produce(wrapper, context, Config.OUT_TOPIC_2, TestMessage.class);
        delay();
        commitTransaction(context);

        return null;
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

    private void openTransaction(Context context) throws PulsarClientException {
        PulsarClient client = context
            .getPulsarClientBuilder()
            .enableTransaction(true)
            .build();
        CompletableFuture<Transaction> transactionFuture = client.newTransaction().withTransactionTimeout(1, TimeUnit.SECONDS).build();
        try {
            transaction = transactionFuture.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            log.error("Cannot open transation", e);
            context.getCurrentRecord().fail();
        }
    }

    private void commitTransaction(Context context) {
        try {
            context.getCurrentRecord().ack();
            transaction.commit().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            log.error("Cannot commit transation", e);
            context.getCurrentRecord().fail();
        }
    }

    private void abortTransaction(Context context) {
        try {
            transaction.abort().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            log.error("Cannot abort transation", e);
            context.getCurrentRecord().fail();
        }
    }
}
