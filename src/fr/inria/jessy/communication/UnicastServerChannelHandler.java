package fr.inria.jessy.communication;

import java.util.HashMap;

import net.sourceforge.fractal.Learner;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.handler.codec.serialization.ObjectDecoder;
import org.jboss.netty.handler.codec.serialization.ObjectEncoder;

import fr.inria.jessy.communication.message.ReadRequestMessage;

public class UnicastServerChannelHandler extends SimpleChannelHandler {

	Learner learner;

	private HashMap<Integer, Channel> swid2Channel = new HashMap<Integer, Channel>();

	public UnicastServerChannelHandler(Learner learner,
			HashMap<Integer, Channel> swid2Channel) {
		this.learner = learner;
		this.swid2Channel = swid2Channel;
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
		swid2Channel.put(msg.source, e.getChannel());
		learner.learn(null, msg);

	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
		System.out.println(e.toString());
	}
}
