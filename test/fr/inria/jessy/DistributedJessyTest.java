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
import net.sourceforge.fractal.utils.PerformanceProbe.ValueRecorder;

import org.junit.Before;
import org.junit.Test;

import com.yahoo.ycsb.YCSBEntity;

import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.vector.NullVector;

public class DistributedJessyTest {

	@Before
	public void setUp() throws ClassNotFoundException {
		MessageStream.addClass(YCSBEntity.class.getName());
		MessageStream.addClass(JessyEntity.class.getName());
	}
	
	@Test
	public void marshallingTest(){
		YCSBEntity e;
		TimeRecorder r = new TimeRecorder("marshallingTime");
		ValueRecorder s = new ValueRecorder("marshallingSize");
		s.setFormat("%a");
		
		PerformanceProbe.setOutput("/dev/stdout");
		for(int i=0; i<1; i++){
			e =  new YCSBEntity();
			e.setLocalVector(new NullVector<String>());
			r.start();
			ByteBuffer bb=pack(e);
			r.stop();
			s.add(bb.array().length);
		}
	}

	@Test
	public void unmarshallingTest(){
		YCSBEntity e;
		TimeRecorder r = new TimeRecorder("unmarshallingTime");
		PerformanceProbe.setOutput("/dev/stdout");
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
