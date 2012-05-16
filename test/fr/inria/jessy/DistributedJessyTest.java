package fr.inria.jessy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import net.sourceforge.fractal.MessageInputStream;
import net.sourceforge.fractal.MessageOutputStream;
import net.sourceforge.fractal.MessageStream;
import net.sourceforge.fractal.utils.PerformanceProbe;
import net.sourceforge.fractal.utils.PerformanceProbe.TimeRecorder;

import org.junit.Before;
import org.junit.Test;

import com.yahoo.ycsb.YCSBEntity;

import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.vector.DependenceVector;
import fr.inria.jessy.vector.NullVector;
import fr.inria.jessy.vector.ValueVector;
import fr.inria.jessy.vector.Vector;

public class DistributedJessyTest {

	@Before
	public void setUp() throws ClassNotFoundException {
		MessageStream.addClass(JessyEntity.class.getName());
		MessageStream.addClass(YCSBEntity.class.getName());
		MessageStream.addClass(Vector.class.getName());
		MessageStream.addClass(ValueVector.class.getName());
		MessageStream.addClass(DependenceVector.class.getName());
		MessageStream.addClass(NullVector.class.getName());
	}
	
	@Test
	public void marshallingTest(){
		YCSBEntity e;
		TimeRecorder r = new TimeRecorder("marshallingTime");
		PerformanceProbe.setOutput("/dev/stdout");
		for(int i=0; i<200000; i++){
			e =  new YCSBEntity();
			r.start();
			pack(e);
			r.stop();
		}
	}

	@Test
	public void unmarshallingTest(){
		YCSBEntity e;
		TimeRecorder r = new TimeRecorder("unmarshallingTime");
		PerformanceProbe.setOutput("/dev/stdout");
		for(int i=0; i<200000; i++){
			e =  new YCSBEntity();
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
