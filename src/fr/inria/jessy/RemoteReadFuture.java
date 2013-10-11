package fr.inria.jessy;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.ReadReply;
import fr.inria.jessy.store.ReadRequest;

public class RemoteReadFuture<E extends JessyEntity> implements
			Future<ReadReply<E>> {

		private Integer state; // 0 => init, 1 => done, -1 => cancelled
		private ReadReply<E> reply;
		private ReadRequest<E> readRequest;
		private volatile boolean done=false;

		public RemoteReadFuture(ReadRequest<E> rr) {
			state = new Integer(0);
			reply = null;
			readRequest = rr;
		}

		public boolean cancel(boolean mayInterruptIfRunning) {
			synchronized (state) {
				if (state != 0)
					return false;
				state = -1;
				if (mayInterruptIfRunning)
					state.notifyAll();
			}
			return true;
		}

		public ReadReply<E> get() throws InterruptedException,
				ExecutionException {
			synchronized (state) {
				if (state == 0 && !done)
					state.wait();
			}
			return (state == -1) ? null : reply;
		}

		public ReadReply<E> get(long timeout, TimeUnit unit)
				throws InterruptedException, ExecutionException,
				TimeoutException {
			
			synchronized (state) {
				if (state == 0 && !done)
					state.wait(timeout);
			}
			return (state == -1) ? null : reply;
		}

		public boolean isCancelled() {
			return state == -1;
		}

		public boolean isDone() {
			return reply == null;
		}

		public boolean mergeReply(ReadReply<E> r) {

			synchronized (state) {

				if (state == -1){
					done=true;
					return true;
				}

				if (reply == null) {
					reply = r;
				} else {
					reply.mergeReply(r);
				}

				if (readRequest.isOneKeyRequest()
						|| reply.getEntity().size() == readRequest
								.getMultiKeys().size()) {
					done=true;
					state.notifyAll();
					
					return true;
				}
				
				return false;
			}

		}

		public ReadRequest<E> getReadRequest() {
			return readRequest;
		}

	}