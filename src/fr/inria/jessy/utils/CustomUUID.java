package fr.inria.jessy.utils;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class CustomUUID {

	private static long localIP;
	private static AtomicLong localCounter = new AtomicLong(System.currentTimeMillis());
	static {
		try {
//			localIP = Long.parseLong(InetAddress.getLocalHost()
//					.getHostAddress().replace(".", ""));
			localIP=			UUID.randomUUID().getLeastSignificantBits();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static UUID getNextUUID() {
		return new UUID(localIP, localCounter.getAndIncrement());
	}

}
