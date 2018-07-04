package simpledb;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
			acquireReadLock(tid, idx);
		}else{
			acquireWriteLock(tid, idx);
		}
	}
	
	public boolean unlockPage(TransactionId tid, int idx) throws InterruptedException{
		return releaseWriteLock(tid, idx) || releaseReadLock(tid, idx);
	}
	
	private void acquireReadLock(TransactionId tid, int idx) throws InterruptedException{
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
	
	private boolean releaseReadLock(TransactionId tid, int idx) throws InterruptedException{
		if(!isHoldLock(tid, idx)){
			return false;
		}
		synchronized (mutexes[idx]) {
			readLockHolders.get(idx).remove(tid);
			if(readLockHolders.get(idx).size() == 0){
				mutexes[idx].notifyAll();
			}
			return true;
		}
	}
	
	private void acquireWriteLock(TransactionId tid, int idx) throws InterruptedException{
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
	
	private boolean releaseWriteLock(TransactionId tid, int idx){
		if(!isHoldLock(tid, idx)){
			return false;
		}
		synchronized (mutexes[idx]) {
			writeLockHolders.set(idx, null);
			mutexes[idx].notifyAll();
			return true;
		}
	}
		
	public boolean isHoldLock(TransactionId tid, int idx){
		return holdWriteLock(tid, idx) || holdReadLock(tid, idx);
	}
	
	private boolean holdWriteLock(TransactionId tid, int idx){
		synchronized(mutexes[idx]) {
			return tid.equals(writeLockHolders.get(idx));
		}
	}
	
	private boolean holdReadLock(TransactionId tid, int idx){
		synchronized(mutexes[idx]) {
			return readLockHolders.get(idx).contains(tid);
		}
	}
}
