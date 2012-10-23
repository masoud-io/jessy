package fr.inria.jessy.communication;

import java.net.InetSocketAddress;

import net.sourceforge.fractal.utils.ExecutorPool;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import fr.inria.jessy.NettyRemoteReader;

public class UnicastServerManager {

	public UnicastServerManager(final NettyRemoteReader learner) {

		ChannelFactory factory = new NioServerSocketChannelFactory(ExecutorPool
				.getInstance().getExecutorService(), ExecutorPool.getInstance()
				.getExecutorService(), Runtime.getRuntime()
				.availableProcessors() - 1);

		ServerBootstrap bootstrap = new ServerBootstrap(factory);

		bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
			public ChannelPipeline getPipeline() {
				return Channels.pipeline(new UnicastServerChannelHandler(
						learner));
			}
		});

		bootstrap.setOption("child.tcpNoDelay", true);
		bootstrap.setOption("child.keepAlive", true);

		bootstrap.bind(new InetSocketAddress(4256));

	}

}
