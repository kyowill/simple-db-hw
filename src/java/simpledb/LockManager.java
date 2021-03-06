package simpledb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
	private final int MIN_TIME = 100, MAX_TIME = 1000;
	private final ConcurrentHashMap<PageId, Object> locks;
	private final Map<PageId, Set<TransactionId>> readLockHolders;
	private final Map<PageId, TransactionId> writeLockHolders;
	private final Map<TransactionId, List<PageId>> dirtyPages; 
	
	public LockManager(){
		locks = new ConcurrentHashMap<PageId, Object>();
		readLockHolders = new ConcurrentHashMap<PageId, Set<TransactionId>>();
		writeLockHolders = new ConcurrentHashMap<PageId, TransactionId>();
		dirtyPages = new ConcurrentHashMap<TransactionId, List<PageId>>();
	}

	public void acquireLock(TransactionId tid,Permissions perm, PageId pid) throws InterruptedException {
		if(dirtyPages.get(tid) == null){
			dirtyPages.put(tid, new ArrayList<PageId>());
		}
		if(readLockHolders.get(pid) == null){
			readLockHolders.put(pid, new HashSet<TransactionId>());
		}
		if(perm.permLevel == 0 && holdReadLock(tid, pid)){
			return;
		}
		if(perm.permLevel == 1 && holdWriteLock(tid, pid)){
			return;
		}
		if(perm.permLevel == 0){
			acquireReadLock(tid, pid);
		}else{
			acquireWriteLock(tid, pid);
		}
		dirtyPages.get(tid).add(pid);
	}

	
	public void releaseLock(TransactionId tid, PageId pid) throws InterruptedException{
		if(holdReadLock(tid, pid)){
			releaseReadLock(tid, pid);
		}
		if(holdWriteLock(tid, pid)){
			releaseWriteLock(tid, pid);
		}
		dirtyPages.get(tid).remove(pid);
	}
	
	private void acquireReadLock(TransactionId tid, PageId pid) throws InterruptedException {
		locks.putIfAbsent(pid, tid);
		synchronized (locks.get(pid)) {
			final Thread thread = Thread.currentThread();
			final Timer timer = new Timer(true);
			
			timer.schedule(new TimerTask() {
				@Override public void run() {
					thread.interrupt();
				}
			}, MIN_TIME);
			while(writeLockHolders.get(pid) != null){
				if(tid.equals(writeLockHolders.get(pid))){
					break;
				}
				locks.get(pid).wait();
			}
			readLockHolders.get(pid).add(tid);
			//System.out.println("read:pid :" + pid.toString() + "tid:" + tid.toString());
			timer.cancel();
		}
	}
	
	private void releaseReadLock(TransactionId tid, PageId pid) throws InterruptedException{
		synchronized (locks.get(pid)) {
			readLockHolders.get(pid).remove(tid);
			if(readLockHolders.get(pid).size() == 0){
				locks.get(pid).notifyAll();
			}
		}
	}
	
	private void acquireWriteLock(TransactionId tid, PageId pid) throws InterruptedException{
		locks.putIfAbsent(pid, tid);
		synchronized (locks.get(pid)) {
			final Thread thread = Thread.currentThread();
			final Timer timer = new Timer(true);
			
			timer.schedule(new TimerTask() {
				@Override public void run() {
					thread.interrupt();
				}
			}, MIN_TIME);
			while(readLockHolders.get(pid).size() != 0 || writeLockHolders.get(pid) != null){
				if(readLockHolders.get(pid).size() == 1 && readLockHolders.get(pid).contains(tid)){
					break;
				}
				locks.get(pid).wait();
			}
			writeLockHolders.put(pid, tid);
			//System.out.println("write:pid :" + pid.toString() + "tid:" + tid.toString());
			timer.cancel();
		}
	}
	
	private void releaseWriteLock(TransactionId tid, PageId pid){
		synchronized (locks.get(pid)) {
			writeLockHolders.remove(pid, tid);
			locks.get(pid).notifyAll();
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
			return readLockHolders.get(pid).contains(tid);
		}
	}
	
	public void releaseLock(TransactionId tid){
		List<PageId> pageIds = new ArrayList<PageId>(dirtyPages.get(tid));
		for(PageId pid: pageIds){
			try {
				releaseLock(tid, pid);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		dirtyPages.put(tid, new ArrayList<PageId>());
	}
}
