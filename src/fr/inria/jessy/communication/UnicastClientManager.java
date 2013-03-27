package fr.inria.jessy.communication;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import net.sourceforge.fractal.membership.Membership;
import net.sourceforge.fractal.utils.ExecutorPool;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.serialization.ObjectDecoder;
import org.jboss.netty.handler.codec.serialization.ObjectEncoder;

import fr.inria.jessy.DistributedJessy;

public class UnicastClientManager {
	private static ChannelFactory factory = new NioClientSocketChannelFactory(ExecutorPool.getInstance().getExecutorService(),
			ExecutorPool.getInstance().getExecutorService(),Runtime.getRuntime().availableProcessors());

	private Map<Integer, Channel> swid2Channel = new HashMap<Integer, Channel>();

	UnicastLearner learner;
	int port;
	
	DistributedJessy distributedJessy;

	public UnicastClientManager(DistributedJessy j, UnicastLearner learner, int port,
			Set<Integer> server_swid) {
		this.learner = learner;
		this.port = port;
		this.distributedJessy=j;

		if (server_swid != null && server_swid.size() > 0) {
			Membership membership = j.manager.getMembership();

			for (Integer swid : server_swid) {
				String host = membership.adressOf(swid);
				swid2Channel.put(swid, createUnicastClientChannel(host, port));

			}
		}
	}

	public synchronized void close() {
			for (Integer i: swid2Channel.keySet()){
				swid2Channel.get(i).close();
				swid2Channel.remove(i);
			}
			swid2Channel.clear();
	}

	
	private Channel createUnicastClientChannel(String host, int port) {

		try {


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

		try{
			Channel ch=swid2Channel.get(swid);
			if (!ch.isConnected()){
				ch.close().awaitUninterruptibly();
				Membership membership = distributedJessy.manager.getMembership();
				String host = membership.adressOf(swid);
				ch=createUnicastClientChannel(host, port);
				swid2Channel.put(swid,ch);
			}
			
			if (ch.isConnected()){
				swid2Channel.get(swid).write(m);
			}
			else{
				System.out.println("Exception... Cannot create a connected channel. Unicast message is being dropped.");
			}
			

		}
		catch(Exception ex){
			ex.printStackTrace();
		}
	}

	public void unicast(Object m, int swid, String destinationHost) {
		try {
			Channel ch =swid2Channel.get(swid);
			
			if (ch==null || !ch.isConnected()) {
				synchronized (swid2Channel) {
					
					ch =swid2Channel.get(swid);
					if (ch==null || !ch.isConnected()) {
						
						if (ch!=null && !ch.isConnected()){
							ch.close().awaitUninterruptibly();
						}
						
						ch=createUnicastClientChannel(destinationHost,port);
						swid2Channel.put(swid,ch);
						System.out.println("UnicastClientManager Is Creating Connection "
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