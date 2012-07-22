package fr.inria.jessy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import net.sourceforge.fractal.Message;
import net.sourceforge.fractal.MessageInputStream;
import net.sourceforge.fractal.MessageOutputStream;
import net.sourceforge.fractal.MessageStream;
import net.sourceforge.fractal.utils.PerformanceProbe;
import net.sourceforge.fractal.utils.PerformanceProbe.TimeRecorder;
import net.sourceforge.fractal.utils.PerformanceProbe.ValueRecorder;

import org.junit.Before;
import org.junit.Test;

import com.yahoo.ycsb.YCSBEntity;

import fr.inria.jessy.communication.message.ParallelSnapshotIsolationPropagateMessage;
import fr.inria.jessy.communication.message.ReadReplyMessage;
import fr.inria.jessy.communication.message.ReadRequestMessage;
import fr.inria.jessy.communication.message.TerminateTransactionRequestMessage;
import fr.inria.jessy.communication.message.VoteMessage;
import fr.inria.jessy.store.EntitySet;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.Keyspace;
import fr.inria.jessy.store.ReadReply;
import fr.inria.jessy.store.ReadRequest;
import fr.inria.jessy.store.ReadRequestKey;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.TransactionHandler;
import fr.inria.jessy.transaction.termination.Vote;
import fr.inria.jessy.vector.CompactVector;
import fr.inria.jessy.vector.ConcurrentVersionVector;
import fr.inria.jessy.vector.DependenceVector;
import fr.inria.jessy.vector.LightScalarVector;
import fr.inria.jessy.vector.NullVector;
import fr.inria.jessy.vector.ValueVector;
import fr.inria.jessy.vector.Vector;

public class DistributedJessyTest {

	@Before
	public void setUp() throws ClassNotFoundException {
		
		PerformanceProbe.setOutput("/dev/stdout");
		
		MessageStream.addClass(JessyEntity.class.getName());
		MessageStream.addClass(YCSBEntity.class.getName());
		MessageStream.addClass(ReadRequestKey.class.getName());
		
		MessageStream.addClass(Vector.class.getName());
		MessageStream.addClass(ValueVector.class.getName());
		MessageStream.addClass(DependenceVector.class.getName());
		MessageStream.addClass(NullVector.class.getName());
		MessageStream.addClass(CompactVector.class.getName());
		MessageStream.addClass(LightScalarVector.class.getName());
//		MessageStream.addClass(VersionVector.class.getName());
		MessageStream.addClass(DependenceVector.class.getName());
		MessageStream.addClass(ConcurrentVersionVector.class.getName());
		
		MessageStream.addClass(ReadReply.class.getName());
		MessageStream.addClass(ReadRequest.class.getName());
		MessageStream.addClass(ReadRequestMessage.class.getName());
		MessageStream.addClass(ReadReplyMessage.class.getName());
		MessageStream.addClass(ParallelSnapshotIsolationPropagateMessage.class.getName());
		
		MessageStream.addClass(VoteMessage.class.getName());
		MessageStream.addClass(Vote.class.getName());
		
		MessageStream.addClass(TerminateTransactionRequestMessage.class.getName());
		
		MessageStream.addClass(ExecutionHistory.class.getName());
		MessageStream.addClass(TransactionHandler.class.getName());
		MessageStream.addClass(EntitySet.class.getName());
		
		MessageStream.addClass(Keyspace.class.getName());
	}
	
	// TerminationRequest
	
	@Test
	public void testTerminationRequest(){		

		TimeRecorder r = new TimeRecorder("marshallingTimeTerminationRequest");
		ValueRecorder s = new ValueRecorder("sizeTerminationRequest");
		s.setFormat("%a");

		ExecutionHistory h = new ExecutionHistory(new TransactionHandler());
		
		YCSBEntity e = new YCSBEntity("user1");
		e.put("a", new String(new byte[1000]));
		h.addReadEntity(e);
		
		YCSBEntity e1 = new YCSBEntity("user4");
		e1.put("a", new String(new byte[1000]));
		h.addReadEntity(e1);
		
		YCSBEntity e2 = new YCSBEntity("user3");
		e2.put("a", new String(new byte[1000]));
		h.addReadEntity(e2);
		
		YCSBEntity e3 = new YCSBEntity("user2");
		e3.put("a", new String(new byte[1000]));
		h.addReadEntity(e3);

		List<String> dest = new ArrayList<String>();
		dest.add("him");
		TerminateTransactionRequestMessage msg = new TerminateTransactionRequestMessage(h,dest,"me",1);
		
		for(int i=0; i<1; i++){
			
			r.start();
			ByteBuffer bb = Message.pack(msg, 1);
			r.stop();
			s.add(bb.array().length);
		}
		
	}	
	
	// ReadReply
	
	@Test
	public void testReadReply(){
		
		YCSBEntity e = new YCSBEntity("user1");
		String payload =  new String(new byte[1000]);
		e.put("a",payload);
		e.setLocalVector(new NullVector<String>());
		
		List<YCSBEntity> l = new ArrayList<YCSBEntity>();
		l.add(e);
		
		ReadReply<YCSBEntity> rr = new ReadReply<YCSBEntity>(l,0);
		List<ReadReply<YCSBEntity>> L = new ArrayList<ReadReply<YCSBEntity>>();
		L.add(rr);
		
		ReadReplyMessage<YCSBEntity> msg = new ReadReplyMessage<YCSBEntity>(L);

		TimeRecorder r = new TimeRecorder("marshallingTimeReadReply");
		TimeRecorder r1 = new TimeRecorder("unmarshallingTimeReadReply");
		ValueRecorder s = new ValueRecorder("sizeRadReply");
		s.setFormat("%a");
		for(int i=0; i<1; i++){
			r.start();
			ByteBuffer bb = Message.pack(msg,0);
			bb.rewind();
			r.stop();
			s.add(bb.limit());
			r1.start();
			try {
				Message.unpack(bb);
			} catch (IOException e1) {
				e1.printStackTrace();
			} catch (ClassNotFoundException e1) {
				e1.printStackTrace();
			}
			r1.stop();
		}		
	}


	// ReadRequest
	@Test
	public void testReadRequest(){
		
		ReadRequestKey<String> k = new ReadRequestKey<String>(null,"user1");;  
		List<ReadRequestKey<String>> l = new ArrayList<ReadRequestKey<String>>();
		l.add(k);
		ReadRequest rr = new ReadRequest(YCSBEntity.class,"secondaryKey",l,null);
		List<ReadRequest<JessyEntity>> L =  new ArrayList<ReadRequest<JessyEntity>>();;
		L.add(rr);		
		ReadRequestMessage msg = new ReadRequestMessage(L);
		
		TimeRecorder r = new TimeRecorder("marshallingTimeReadRequest");
		TimeRecorder r1 = new TimeRecorder("unmarshallingTimeReadRequest");
		ValueRecorder s = new ValueRecorder("sizeRadRequest");
		s.setFormat("%a");
		for(int i=0; i<100000; i++){
			r.start();
			ByteBuffer bb = Message.pack(msg,0);
			bb.rewind();
			r.stop();
			s.add(bb.limit());
			r1.start();
			try {
				Message.unpack(bb);
			} catch (IOException e1) {
				e1.printStackTrace();
			} catch (ClassNotFoundException e1) {
				e1.printStackTrace();
			}
			r1.stop();
		}
	}
	
	// JessyEntity.

	@Test
	public void testJessyEntity(){
		YCSBEntity e;
		TimeRecorder r = new TimeRecorder("unmarshallingTimeJessyEntity");
		for(int i=0; i<1; i++){
			e =  new YCSBEntity();
			e.setLocalVector(new NullVector<String>());
			ByteBuffer bb = pack(e);
			r.start();
			try {
				unpack(bb.array());
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (ClassNotFoundException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			r.stop();
		}		
	}

	
	private static JessyEntity unpack(byte[] buff) throws IOException, ClassNotFoundException{
		ByteArrayInputStream bais = new ByteArrayInputStream(buff);
		MessageInputStream mis = new MessageInputStream(bais);
		JessyEntity e = (JessyEntity)mis.readObject();
		return e;
	}
	
	private static ByteBuffer pack(JessyEntity e) {

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		MessageOutputStream mos;
		try {
			mos = new MessageOutputStream(baos);
			mos.writeObject(e);
		} catch (IOException exception) {
			// TODO Auto-generated catch block
			exception.printStackTrace();
		}
		byte [] data = baos.toByteArray(); 

		// 3 - Pack into a ByteBuffer
		ByteBuffer bb = ByteBuffer.allocate(data.length);
		bb.put(data);
		bb.flip();
		return bb;
	}
	
}
