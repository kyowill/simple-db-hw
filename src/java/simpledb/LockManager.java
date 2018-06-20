package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;

public class LockManager {
	private final Object[] mutexes;
	private final List<Set<TransactionId>> readLockHolders;
	private final List<TransactionId> writeLockHolders;
	private final int MIN_TIME = 100, MAX_TIME = 1000;
	
	public LockManager(int max) {
		mutexes = new Object[max];
		readLockHolders = new ArrayList<Set<TransactionId>>(max);
		writeLockHolders = new ArrayList<TransactionId>(max);
		for (int i=0; i<max; i++) {
			mutexes[i] = new Object();
			readLockHolders.add(new HashSet<TransactionId>());
			writeLockHolders.add(null);
		}
	}
	
	public void lockPage(TransactionId tid, Permissions perm, int idx) throws InterruptedException{
		if(perm.permLevel == 0){
			acquireReadlock(tid, idx);
		}else{
			acquireWritelock(tid, idx);
		}
	}
	
	public void unlockPage(TransactionId tid, Permissions perm, int idx) throws InterruptedException{
		if(perm.permLevel == 0){
			releaseReadlock(tid, idx);
		}else{
			releaseWritelock(tid, idx);
		}
	}
	
	private void acquireReadlock(TransactionId tid, int idx) throws InterruptedException{
		if(isHoldLock(tid, idx)){
			return;
		}
		synchronized (mutexes[idx]) {
			while(writeLockHolders.get(idx) != null){
				mutexes[idx].wait();
			}
			readLockHolders.get(idx).add(tid);
		}
	}
	
	private void releaseReadlock(TransactionId tid, int idx) throws InterruptedException{
		if(!isHoldLock(tid, idx)){
			return;
		}
		synchronized (mutexes[idx]) {
			readLockHolders.get(idx).remove(tid);
			if(readLockHolders.get(idx).size() == 0){
				mutexes[idx].notifyAll();
			}
		}
	}
	
	private void acquireWritelock(TransactionId tid, int idx) throws InterruptedException{
		if(isHoldLock(tid, idx)){
			return;
		}
		synchronized (mutexes[idx]) {
			while(readLockHolders.get(idx).size() != 0){
				mutexes[idx].wait();
			}
			writeLockHolders.set(idx, tid);
		}
	}
	
	private void releaseWritelock(TransactionId tid, int idx){
		if(!isHoldLock(tid, idx)){
			return;
		}
		synchronized (mutexes[idx]) {
			writeLockHolders.set(idx, null);
			mutexes[idx].notifyAll();
		}
	}
		
	public boolean isHoldLock(TransactionId tid, int idx){
		return holdsWriteLock(tid, idx) || holdsReadLock(tid, idx);
	}
	
	private boolean holdsWriteLock(TransactionId tid, int idx){
		synchronized(mutexes[idx]) {
			return tid.equals(writeLockHolders.get(idx));
		}
	}
	
	private boolean holdsReadLock(TransactionId tid, int idx){
		synchronized(mutexes[idx]) {
			return readLockHolders.get(idx).contains(tid);
		}
	}
}
