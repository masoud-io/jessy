package fr.inria.jessy.communication;

import org.jboss.netty.channel.Channel;

public interface UnicastLearner {

	public void receiveMessage(Object message, Channel channel);
}
