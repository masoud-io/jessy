package fr.inria.jessy.communication;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import net.sourceforge.fractal.membership.Membership;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.serialization.ObjectDecoder;
import org.jboss.netty.handler.codec.serialization.ObjectEncoder;

public class UnicastClientManager {

	private Map<Integer, Channel> swid2Channel = new HashMap<Integer, Channel>();

	UnicastLearner learner;
	int port;

	public UnicastClientManager(UnicastLearner learner, int port,
			Set<Integer> server_swid) {
		this.learner = learner;
		this.port = port;

		if (server_swid != null && server_swid.size() > 0) {
			Membership membership = JessyGroupManager.getInstance()
					.getMembership();

			for (Integer swid : server_swid) {
				String host = membership.adressOf(swid);
				swid2Channel.put(swid, createUnicastClientChannel(host, port));

			}
		}
	}

	public void closeConnections() {
		// TODO
	}

	private Channel createUnicastClientChannel(String host, int port) {

		try {

			ChannelFactory factory = new NioClientSocketChannelFactory();

			ClientBootstrap bootstrap = new ClientBootstrap(factory);

			bootstrap.setOption("tcpNoDelay", true);
			bootstrap.setOption("keepAlive", true);

			bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
				public ChannelPipeline getPipeline() throws Exception {
					ChannelPipeline pipeline = Channels.pipeline();
					pipeline.addLast("decoder", new ObjectDecoder());
					pipeline.addLast("encoder", new ObjectEncoder());
					pipeline.addLast("handler",
							new UnicastClientChannelHandler(learner));
					return pipeline;
				}
			});

			// Connect to the server, wait for the connection and get back the
			// channel
			return bootstrap.connect(new InetSocketAddress(host, port))
					.awaitUninterruptibly().getChannel();

		} catch (Exception ex) {
			ex.printStackTrace();
		}

		return null;

	}

	public void unicast(Object m, int swid) throws NullPointerException {
		if (!swid2Channel.containsKey(swid)) {
			throw new NullPointerException(
					"Cannot identify the host name with swid from Fractal membership");
		}

		Channel ch=swid2Channel.get(swid);
		if (ch.isConnected()){
			swid2Channel.get(swid).write(m);
		}
		else{
			Membership membership = JessyGroupManager.getInstance()
					.getMembership();
			String host = membership.adressOf(swid);
			swid2Channel.put(swid, createUnicastClientChannel(host, port));
		}
	}

	public void unicast(Object m, int swid, String destinationHost) {
		try {
			if (!swid2Channel.containsKey(swid)) {
				synchronized (swid2Channel) {
					if (!swid2Channel.containsKey(swid)) {
						swid2Channel.put(
								swid,
								createUnicastClientChannel(destinationHost,
										port));
						System.out.println("Creating Connection "
								+ destinationHost);
					}
				}
			}

			swid2Channel.get(swid).write(m);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

}
