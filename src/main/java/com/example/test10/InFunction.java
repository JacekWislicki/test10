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

class InFunction implements Function<TestMessage, Void> {

    private Transaction transaction;

    @Override
    public Void process(TestMessage input, Context context) throws PulsarClientException {
        if (transaction != null) {
            abortTransaction(context);
        }

        MessageWrapper<TestMessage> wrapper = new MessageWrapper<>(input, new Metadata());
        openTransaction(context);
        FunctionUtils.produce(wrapper, context, Config.OUT_TOPIC_1, null);
        FunctionUtils.produce(wrapper, context, Config.OUT_TOPIC_2, null);
        commitTransaction(context);

        return null;
    }

    private void openTransaction(Context context) throws PulsarClientException {
        PulsarClient client = context.getPulsarClient();
        CompletableFuture<Transaction> transactionFuture = client.newTransaction().withTransactionTimeout(1, TimeUnit.SECONDS).build();
        try {
            transaction = transactionFuture.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            // TODO log exception?
            context.getCurrentRecord().fail();
        }
    }

    private void commitTransaction(Context context) {
        try {
            transaction.commit().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            // TODO log exception?
            context.getCurrentRecord().fail();
        }
    }

    private void abortTransaction(Context context) {
        try {
            transaction.abort().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            // TODO log exception?
            context.getCurrentRecord().fail();
        }
    }
}
