package fr.inria.jessy.communication;

import net.sourceforge.fractal.Learner;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.handler.codec.serialization.ObjectDecoder;
import org.jboss.netty.handler.codec.serialization.ObjectEncoder;

import fr.inria.jessy.communication.message.ReadReplyMessage;

public class UnicastClientChannelHandler extends SimpleChannelHandler {

	Learner learner;

	public UnicastClientChannelHandler(Learner learner) {
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
		ReadReplyMessage msg = (ReadReplyMessage) e.getMessage();
		learner.learn(null, msg);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
		e.getCause().printStackTrace();
	}
}
