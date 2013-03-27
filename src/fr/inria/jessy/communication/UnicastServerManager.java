package fr.inria.jessy.communication;

import java.net.InetSocketAddress;

import net.sourceforge.fractal.utils.ExecutorPool;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.serialization.ObjectDecoder;
import org.jboss.netty.handler.codec.serialization.ObjectEncoder;

import fr.inria.jessy.DistributedJessy;

public class UnicastServerManager {
	private static ChannelFactory factory = new NioServerSocketChannelFactory(ExecutorPool.getInstance().getExecutorService(),
			ExecutorPool.getInstance().getExecutorService(),Runtime.getRuntime().availableProcessors());

	private Channel channel;
	
	public UnicastServerManager(DistributedJessy j, final UnicastLearner learner, int port) {


		ServerBootstrap bootstrap = new ServerBootstrap(factory);

		bootstrap.setOption("child.tcpNoDelay", true);
		bootstrap.setOption("child.keepAlive", true);

		bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
			public ChannelPipeline getPipeline() {
				ChannelPipeline pipeline = Channels.pipeline();
				pipeline.addLast("decoder", new ObjectDecoder());
				pipeline.addLast("encoder", new ObjectEncoder());
				pipeline.addLast("handler", new UnicastServerChannelHandler(
						learner));
				return pipeline;
			}
		});

		String host = j.manager.getMembership().adressOf(j.manager.getSourceId());
		channel=bootstrap.bind(new InetSocketAddress(host, port));

	}

	public void close(){
		channel.close();
	}
}