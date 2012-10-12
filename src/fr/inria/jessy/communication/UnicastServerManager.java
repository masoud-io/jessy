package fr.inria.jessy.communication;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.concurrent.Executors;

import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.multicast.MulticastMessage;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

public class UnicastServerManager {	

	private HashMap<Integer, Channel> swid2Channel = new HashMap<Integer, Channel>();

	public UnicastServerManager(final Learner learner) {

		ChannelFactory factory = new NioServerSocketChannelFactory(
				Executors.newCachedThreadPool(),
				Executors.newCachedThreadPool());

		ServerBootstrap bootstrap = new ServerBootstrap(factory);

		bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
			public ChannelPipeline getPipeline() {
				return Channels.pipeline(new UnicastServerChannelHandler(
						learner, swid2Channel));
			}
		});

		bootstrap.setOption("child.tcpNoDelay", true);
		bootstrap.setOption("child.keepAlive", true);

		bootstrap.bind(new InetSocketAddress(4256));

	}

	public void unicast(MulticastMessage m, int swid) {
		Channel ch = swid2Channel.get(m.source);
		ch.write(m);
	}
}
