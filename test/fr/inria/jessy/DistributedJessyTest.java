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
import fr.inria.jessy.vector.VersionVector;

public class DistributedJessyTest {

	@Before
	public void setUp() throws ClassNotFoundException {
		
		MessageStream.addClass(JessyEntity.class.getName());
		MessageStream.addClass(YCSBEntity.class.getName());
		
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
	
	@Test
	public void marshallingTest(){		

		TimeRecorder r = new TimeRecorder("marshallingTime");
		ValueRecorder s = new ValueRecorder("marshallingSize");
		s.setFormat("%a");
		
		PerformanceProbe.setOutput("/dev/stdout");
		for(int i=0; i<10000; i++){

			YCSBEntity e = new YCSBEntity("user1");
			ReadReply<JessyEntity> rr = new ReadReply<JessyEntity>(e, 1);
			List<ReadReply<JessyEntity>> l = new ArrayList<ReadReply<JessyEntity>>();
			l.add(rr);
			ReadReplyMessage msg = new ReadReplyMessage<JessyEntity>(l);
			
			r.start();
			ByteBuffer bb = Message.pack(msg, 1);
			r.stop();
			s.add(bb.array().length);
		}
	}

//	@Test
//	public void unmarshallingTest(){
//		YCSBEntity e;
//		TimeRecorder r = new TimeRecorder("unmarshallingTime");
//		PerformanceProbe.setOutput("/dev/stdout");
//		for(int i=0; i<1; i++){
//			e =  new YCSBEntity();
//			e.setLocalVector(new NullVector<String>());
//			ByteBuffer bb = pack(e);
//			r.start();
//			try {
//				unpack(bb.array());
//			} catch (IOException e1) {
//				// TODO Auto-generated catch block
//				e1.printStackTrace();
//			} catch (ClassNotFoundException e1) {
//				// TODO Auto-generated catch block
//				e1.printStackTrace();
//			}
//			r.stop();
//		}		
//	}
//
//	private static JessyEntity unpack(byte[] buff) throws IOException, ClassNotFoundException{
//		ByteArrayInputStream bais = new ByteArrayInputStream(buff);
//		MessageInputStream mis = new MessageInputStream(bais);
//		JessyEntity e = (JessyEntity)mis.readObject();
//		return e;
//	}
//	
//	private static ByteBuffer pack(JessyEntity e) {
//
//		ByteArrayOutputStream baos = new ByteArrayOutputStream();
//		MessageOutputStream mos;
//		try {
//			mos = new MessageOutputStream(baos);
//			mos.writeObject(e);
//		} catch (IOException exception) {
//			// TODO Auto-generated catch block
//			exception.printStackTrace();
//		}
//		byte [] data = baos.toByteArray(); 
//
//		// 3 - Pack into a ByteBuffer
//		ByteBuffer bb = ByteBuffer.allocate(data.length);
//		bb.put(data);
//		bb.flip();
//		return bb;
//	}
//	
}
