package com.hebangdata.rmq;

import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class Server {
	private final static Logger log = LoggerFactory.getLogger("Server");

	private final static String RPC_QUEUE_NAME = "rpc_queue";

	public static void main(String[] args) {
		final ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");

		Connection connection = null;
		try {
			connection = factory.newConnection();
			final Channel channel = connection.createChannel();

			channel.queueDeclare(RPC_QUEUE_NAME, false, false, false, null);

			channel.basicQos(1);

			log.info(" [x] Awaiting RPC Requesting");

			final Consumer consumer = new DefaultConsumer(channel) {
				@Override
				public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
					final AMQP.BasicProperties replyProperties = new AMQP.BasicProperties()
							.builder()
							.correlationId(properties.getCorrelationId())
							.build();

					String response = "";

					try {
						final String message = new String(body, "UTF-8");
						final int n = Integer.parseInt(message);

						log.info(" [.] fib({})", message);

						response += fib(n);
					} catch (RuntimeException ex) {
						log.error(" [.] {}", ex);
					} finally {
						channel.basicPublish("", properties.getReplyTo(), replyProperties, response.getBytes("UTF-8"));

						channel.basicAck(envelope.getDeliveryTag(), false);

						synchronized (this) {
							this.notify();
						}
					}
				}
			};

			channel.basicConsume(RPC_QUEUE_NAME, false, consumer);

			while (true) {
				synchronized (consumer) {
					try {
						consumer.wait();
					} catch (InterruptedException e) {
						log.error(" [.] {}", e);
					}
				}
			}
		} catch (IOException | TimeoutException ex) {
			ex.printStackTrace();
		} finally {
			if (null != connection) {
				try {
					connection.close();
				} catch (IOException ex) {
					log.error(" [.] {}", ex);
				}
			}
		}
	}

	private static int fib(final int n) {
		if (0 == n) return 0;
		if (1 == n) return 1;
		return fib(n-1) + fib(n-2);
	}
}
