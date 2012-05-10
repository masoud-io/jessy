package fr.inria.jessy.utils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class CustomUUID {

	private static long localIP;
	private AtomicInteger localCounter = new AtomicInteger();
	static {
		try {
			localIP = Long.parseLong(InetAddress.getLocalHost()
					.getHostAddress().replace(".", ""));
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}

	public UUID getNextUUID() {
		return new UUID(localIP, localCounter.getAndIncrement());
	}

}
