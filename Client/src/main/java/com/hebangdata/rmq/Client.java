package com.hebangdata.rmq;

import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;


public class Client {
	private final static Logger log = LoggerFactory.getLogger("Client");

	private final static String RPC_QUEUE_NAME = "rpc_queue";

	private final Connection connection;
	private final Channel channel;
	private final String replyQueueName;

	public Client(final String host) throws IOException, TimeoutException {
		final ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(host);

		connection = factory.newConnection();
		channel = connection.createChannel();

		replyQueueName = channel.queueDeclare().getQueue();
	}

	public String Call(final String message) throws IOException, InterruptedException {
		final String corrId = UUID.randomUUID().toString();

		final AMQP.BasicProperties properties = new AMQP.BasicProperties()
				.builder()
				.correlationId(corrId)
				.replyTo(replyQueueName)
				.build();

		channel.basicPublish("", RPC_QUEUE_NAME, properties, message.getBytes("UTF-8"));

		final BlockingQueue<String> response = new ArrayBlockingQueue<>(1);

		channel.basicConsume(replyQueueName, true, new DefaultConsumer(channel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
				if (properties.getCorrelationId().equals(corrId)) {
					response.offer(new String(body, "UTF-8"));
				} else {
					log.warn(" [x] Received correlationId was not match: {} - {}", properties.getCorrelationId(), corrId);
				}
			}
		});

		return response.take();
	}

	public void Close() throws IOException, TimeoutException {
		channel.close();
		connection.close();
	}
}
