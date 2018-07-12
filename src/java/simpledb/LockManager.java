package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
	private final int MIN_TIME = 100, MAX_TIME = 1000;
	private final ConcurrentHashMap<PageId, Object> locks;
	private final Map<PageId, Set<TransactionId>> readLockHolders;
	private final Map<PageId, TransactionId> writeLockHolders;
	private final Map<TransactionId, Set<PageId>> dirtyPages; 
	
	public LockManager(){
		locks = new ConcurrentHashMap<PageId, Object>();
		readLockHolders = new ConcurrentHashMap<PageId, Set<TransactionId>>();
		writeLockHolders = new ConcurrentHashMap<PageId, TransactionId>();
		dirtyPages = new ConcurrentHashMap<TransactionId, Set<PageId>>();
	}

	public void acquireLock(TransactionId tid,Permissions perm, PageId pid) throws InterruptedException {
		if(dirtyPages.get(tid) == null){
			dirtyPages.put(tid, new HashSet<PageId>());
		}
		if(readLockHolders.get(pid) == null){
			readLockHolders.put(pid, new HashSet<TransactionId>());
		}
		if(perm.permLevel == 0){
			acquireReadLock(tid, pid);
		}else{
			acquireWriteLock(tid, pid);
		}
	}

	
	public boolean releaseLock(TransactionId tid, PageId pid) throws InterruptedException{
		return releaseWriteLock(tid, pid) || releaseReadLock(tid, pid);
	}
	
	private void acquireReadLock(TransactionId tid, PageId pid) throws InterruptedException {
		if(isHoldLock(tid, pid)){
			return;
		}
		locks.putIfAbsent(pid, tid);
		synchronized (locks.get(pid)) {
			while(writeLockHolders.get(pid) != null){
				locks.get(pid).wait();
			}
			readLockHolders.get(pid).add(tid);
			dirtyPages.get(tid).add(pid);
		}
	}
	
	private boolean releaseReadLock(TransactionId tid, PageId pid) throws InterruptedException{
		if(!isHoldLock(tid, pid)){
			return false;
		}
		synchronized (locks.get(pid)) {
			readLockHolders.get(pid).remove(tid);
			dirtyPages.get(tid).remove(pid);
			if(readLockHolders.get(pid).size() == 0){
				locks.get(pid).notifyAll();
			}
			return true;
		}
	}
	
	private void acquireWriteLock(TransactionId tid, PageId pid) throws InterruptedException{
		if(isHoldLock(tid, pid)){
			return;
		}
		locks.putIfAbsent(pid, tid);
		synchronized (locks.get(pid)) {
			while(readLockHolders.get(pid).size() != 0){
				locks.get(pid).wait();
			}
			writeLockHolders.put(pid, tid);
			dirtyPages.get(tid).add(pid);
		}
	}
	
	private boolean releaseWriteLock(TransactionId tid, PageId pid){
		if(!isHoldLock(tid, pid)){
			return false;
		}
		synchronized (locks.get(pid)) {
			writeLockHolders.remove(pid, tid);
			dirtyPages.get(tid).remove(pid);
			locks.get(pid).notifyAll();
			return true;
		}
	}
	
	public boolean isHoldLock(TransactionId tid, PageId pid){
		return holdWriteLock(tid, pid) || holdReadLock(tid, pid);
	}
	
	private boolean holdWriteLock(TransactionId tid, PageId pid){
		if(locks.get(pid) == null){
			return false;
		}
		synchronized(locks.get(pid)) {
			return tid.equals(writeLockHolders.get(pid));
		}
	}

	private boolean holdReadLock(TransactionId tid, PageId pid){
		if(locks.get(pid) == null){
			return false;
		}
		synchronized(locks.get(pid)) {
			boolean owned  = readLockHolders.get(pid).contains(tid);
			if(owned && readLockHolders.get(pid).size() == 1){
				writeLockHolders.put(pid, tid);
				readLockHolders.get(pid).clear();
			}
			return owned;
		}
	}
	
	public void releaseLock(TransactionId tid){
		if(dirtyPages.get(tid) == null || dirtyPages.get(tid).size() == 0){
			return;
		}
		Iterator<PageId> iter = dirtyPages.get(tid).iterator();
		while(iter.hasNext()){
			PageId pid = iter.next(); 
			if(pid == null){
				continue;
			}
			try {
				releaseLock(tid, pid);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
