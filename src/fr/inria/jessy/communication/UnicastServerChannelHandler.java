package fr.inria.jessy.communication;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

public class UnicastServerChannelHandler extends SimpleChannelHandler {

	UnicastLearner learner;

	public UnicastServerChannelHandler(UnicastLearner learner) {
		this.learner = learner;
	}

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
		learner.receiveMessage(e.getMessage(), e.getChannel());
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
		System.out.println("UnicastServerChannelHandler: " + e.toString());
		e.getCause().printStackTrace();
	}
}