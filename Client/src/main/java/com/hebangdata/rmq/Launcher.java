package com.hebangdata.rmq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class Launcher {
	private final static Logger log = LoggerFactory.getLogger("Launcher");

	public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
		final Client client = new Client("localhost");

		log.info(" [x] Requesting fib(30)");
		final String response = client.Call("30");
		log.info(" [.] Got '{}'", response);

		client.Close();
	}
}
