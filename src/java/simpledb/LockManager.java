package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
//	private final Object[] mutexes;
//	private final List<Set<TransactionId>> readLockHolders;
//	private final List<TransactionId> writeLockHolders;
	private final int MIN_TIME = 100, MAX_TIME = 1000;
	private final ConcurrentHashMap<PageId, Object> locks;
	private final Map<PageId, List<TransactionId>> readLockHolders;
	private final Map<PageId, TransactionId> writeLockHolders;
//	public LockManager(int max) {
//		mutexes = new Object[max];
//		readLockHolders = new ArrayList<Set<TransactionId>>(max);
//		writeLockHolders = new ArrayList<TransactionId>(max);
//		for (int i=0; i<max; i++) {
//			mutexes[i] = new Object();
//			readLockHolders.add(new HashSet<TransactionId>());
//			writeLockHolders.add(null);
//		}
//	}
	
	public LockManager(){
		locks = new ConcurrentHashMap<PageId, Object>();
		readLockHolders = new HashMap<PageId, List<TransactionId>>();
		writeLockHolders = new HashMap<PageId, TransactionId>();
	}
	
//	public void lockPage(TransactionId tid, Permissions perm, int idx) throws InterruptedException{
//		if(perm.permLevel == 0){
//			acquireReadLock(tid, idx);
//		}else{
//			acquireWriteLock(tid, idx);
//		}
//	}
	public void acquireLock(TransactionId tid,Permissions perm, PageId pid) throws InterruptedException {
		if(perm.permLevel == 0){
			acquireReadLock(tid, pid);
		}else{
			acquireWriteLock(tid, pid);
		}
	}
	
//	public boolean unlockPage(TransactionId tid, int idx) throws InterruptedException{
//		return releaseWriteLock(tid, idx) || releaseReadLock(tid, idx);
//	}
	
	public boolean releaseLock(TransactionId tid, PageId pid) throws InterruptedException{
		return releaseWriteLock(tid, pid) || releaseReadLock(tid, pid);
	}
	
//	private void acquireReadLock(TransactionId tid, int idx) throws InterruptedException{
//		if(isHoldLock(tid, idx)){
//			return;
//		}
//		synchronized (mutexes[idx]) {
//			while(writeLockHolders.get(idx) != null){
//				mutexes[idx].wait();
//			}
//			readLockHolders.get(idx).add(tid);
//		}
//	}
	
	private void acquireReadLock(TransactionId tid, PageId pid) throws InterruptedException {
		if(isHoldLock(tid, pid)){
			return;
		}
		locks.putIfAbsent(pid, tid);
		if(readLockHolders.get(pid) == null){
			readLockHolders.put(pid, new ArrayList<TransactionId>());
		}
		synchronized (locks.get(pid)) {
			while(writeLockHolders.get(pid) != null){
				locks.get(pid).wait();
			}
			readLockHolders.get(pid).add(tid);
		}
	}
	
//	private boolean releaseReadLock(TransactionId tid, int idx) throws InterruptedException{
//		if(!isHoldLock(tid, idx)){
//			return false;
//		}
//		synchronized (mutexes[idx]) {
//			readLockHolders.get(idx).remove(tid);
//			if(readLockHolders.get(idx).size() == 0){
//				mutexes[idx].notifyAll();
//			}
//			return true;
//		}
//	}
	
	private boolean releaseReadLock(TransactionId tid, PageId pid) throws InterruptedException{
		if(!isHoldLock(tid, pid)){
			return false;
		}
		synchronized (locks.get(pid)) {
			readLockHolders.get(pid).remove(tid);
			if(readLockHolders.get(pid).size() == 0){
				locks.get(pid).notifyAll();
			}
			return true;
		}
	}
	
//	private void acquireWriteLock(TransactionId tid, int idx) throws InterruptedException{
//		if(isHoldLock(tid, idx)){
//			return;
//		}
//		synchronized (mutexes[idx]) {
//			while(readLockHolders.get(idx).size() != 0){
//				mutexes[idx].wait();
//			}
//			writeLockHolders.set(idx, tid);
//		}
//	}
	
	private void acquireWriteLock(TransactionId tid, PageId pid) throws InterruptedException{
		if(isHoldLock(tid, pid)){
			return;
		}
		locks.putIfAbsent(pid, tid);
		if(readLockHolders.get(pid) == null){
			readLockHolders.put(pid, new ArrayList<TransactionId>());
		}
		synchronized (locks.get(pid)) {
			while(readLockHolders.get(pid).size() != 0){
				locks.get(pid).wait();
			}
			writeLockHolders.put(pid, tid);
		}
	}
	
//	private boolean releaseWriteLock(TransactionId tid, int idx){
//		if(!isHoldLock(tid, idx)){
//			return false;
//		}
//		synchronized (mutexes[idx]) {
//			writeLockHolders.set(idx, null);
//			mutexes[idx].notifyAll();
//			return true;
//		}
//	}
	
	private boolean releaseWriteLock(TransactionId tid, PageId pid){
		if(!isHoldLock(tid, pid)){
			return false;
		}
		synchronized (locks.get(pid)) {
			writeLockHolders.remove(pid, tid);
			locks.get(pid).notifyAll();
			return true;
		}
	}
		
//	public boolean isHoldLock(TransactionId tid, int idx){
//		return holdWriteLock(tid, idx) || holdReadLock(tid, idx);
//	}
	
	public boolean isHoldLock(TransactionId tid, PageId pid){
		return holdWriteLock(tid, pid) || holdReadLock(tid, pid);
	}
	
//	private boolean holdWriteLock(TransactionId tid, int idx){
//		synchronized(mutexes[idx]) {
//			return tid.equals(writeLockHolders.get(idx));
//		}
//	}
	private boolean holdWriteLock(TransactionId tid, PageId pid){
		if(locks.get(pid) == null){
			return false;
		}
		synchronized(locks.get(pid)) {
			return tid.equals(writeLockHolders.get(pid));
		}
	}
	
//	private boolean holdReadLock(TransactionId tid, int idx){
//		synchronized(mutexes[idx]) {
//			return readLockHolders.get(idx).contains(tid);
//		}
//	}
	
	private boolean holdReadLock(TransactionId tid, PageId pid){
		if(locks.get(pid) == null){
			return false;
		}
		synchronized(locks.get(pid)) {
			return readLockHolders.get(pid).contains(tid);
		}
	}
}
