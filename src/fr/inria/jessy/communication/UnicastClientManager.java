package fr.inria.jessy.communication;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.concurrent.Executors;

import net.sourceforge.fractal.membership.Membership;
import net.sourceforge.fractal.multicast.MulticastMessage;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import fr.inria.jessy.NettyRemoteReader;

public class UnicastClientManager {

	private HashMap<Integer, Channel> swid2Channel = new HashMap<Integer, Channel>();

	NettyRemoteReader learner;
	
	public UnicastClientManager(NettyRemoteReader learner) {
		this.learner=learner;
		
		Membership membership = JessyGroupManager.getInstance().getMembership();

		for (Integer swid : JessyGroupManager.getInstance()
				.getAllReplicaGroup().members()) {
			String host = membership.adressOf(swid);
			swid2Channel.put(swid, createUnicastClientChannel(host));

		}
	}

	public void closeConnections() {
		// TODO
	}

	private Channel createUnicastClientChannel(String host) {
		int port = 4256;

		ChannelFactory factory = new NioClientSocketChannelFactory(
				Executors.newCachedThreadPool(),
				Executors.newCachedThreadPool());

		ClientBootstrap bootstrap = new ClientBootstrap(factory);

		// Create the associated Handler
		UnicastClientChannelHandler handler = new UnicastClientChannelHandler(learner);

		// Add the handler to the pipeline and set some options
		bootstrap.getPipeline().addLast("handler", handler);
		bootstrap.setOption("tcpNoDelay", true);
		bootstrap.setOption("keepAlive", true);
		
		bootstrap.setOption("sendBufferSize", 1048576); 
		bootstrap.setOption("receiveBufferSize", 1048576);

		// Connect to the server, wait for the connection and get back the
		// channel
		return bootstrap.connect(new InetSocketAddress(host, port))
				.awaitUninterruptibly().getChannel();

	}

	public void unicast(MulticastMessage m, int swid) {
		swid2Channel.get(swid).write(m);
	}

}
