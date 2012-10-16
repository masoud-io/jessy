package fr.inria.jessy.communication;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.handler.codec.serialization.ObjectDecoder;
import org.jboss.netty.handler.codec.serialization.ObjectEncoder;

import fr.inria.jessy.NettyRemoteReader;
import fr.inria.jessy.communication.message.ReadRequestMessage;

public class UnicastServerChannelHandler extends SimpleChannelHandler {

	NettyRemoteReader learner;


	public UnicastServerChannelHandler(NettyRemoteReader learner) {
		this.learner = learner;
	}

	/**
	 * Add the ObjectXxcoder to the Pipeline
	 */
	@Override
	public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e) {
		e.getChannel().getPipeline().addFirst("decoder", new ObjectDecoder());
		e.getChannel().getPipeline()
				.addAfter("decoder", "encoder", new ObjectEncoder());
	}

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
		ReadRequestMessage msg = (ReadRequestMessage) e.getMessage();
		learner.learnReadRequestMessage(msg, e.getChannel());

	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
		System.out.println(e.toString());
	}
}
