package fr.inria.jessy.communication;

import java.net.InetSocketAddress;
import java.util.HashMap;

import net.sourceforge.fractal.membership.Membership;
import net.sourceforge.fractal.multicast.MulticastMessage;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.serialization.ObjectDecoder;
import org.jboss.netty.handler.codec.serialization.ObjectEncoder;

import fr.inria.jessy.ConstantPool;
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
		
		try{
			int port = ConstantPool.JESSY_NETTY_REMOTE_READER_PORT;
			
			ChannelFactory factory = new NioClientSocketChannelFactory();
			
			ClientBootstrap bootstrap = new ClientBootstrap(factory);
			
			bootstrap.setOption("tcpNoDelay", true);
			bootstrap.setOption("keepAlive", true);
			
			bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
				public ChannelPipeline getPipeline() throws Exception {
					ChannelPipeline pipeline = Channels.pipeline();
					pipeline.addLast("decoder", new ObjectDecoder());
					pipeline.addLast("encoder", new ObjectEncoder());
					pipeline.addLast("handler",new UnicastClientChannelHandler(learner));
					return pipeline;
				}
			});		 
			
			// Connect to the server, wait for the connection and get back the
			// channel
			return bootstrap.connect(new InetSocketAddress(host, port))
					.awaitUninterruptibly().getChannel();
			
		}
		catch(Exception ex){
			ex.printStackTrace();
		}
		
		return null;

	}

	public void unicast(MulticastMessage m, int swid) {
		swid2Channel.get(swid).write(m);
	}

}
