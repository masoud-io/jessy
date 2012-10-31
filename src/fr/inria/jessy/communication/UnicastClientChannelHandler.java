package fr.inria.jessy.communication;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

import fr.inria.jessy.NettyRemoteReader;
import fr.inria.jessy.communication.message.ReadReplyMessage;

public class UnicastClientChannelHandler extends SimpleChannelHandler {

	NettyRemoteReader learner;

	public UnicastClientChannelHandler(NettyRemoteReader learner) {
		this.learner = learner;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
		learner.learnReadReplyMessage((ReadReplyMessage) e.getMessage());
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
		e.getCause().printStackTrace();
	}
}
