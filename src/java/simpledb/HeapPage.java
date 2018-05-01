package simpledb;

import java.util.*;
import java.io.*;

/**
 * Each instance of HeapPage stores data for one page of HeapFiles and
 * implements the Page interface that is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 */
public class HeapPage implements Page {

    final HeapPageId pid;
    final TupleDesc td;
    final byte header[];
    final Tuple tuples[];
    final int numSlots;

    byte[] oldData;
    private final Byte oldDataLock = new Byte((byte) 0);

    private List<Tuple> validTuples = new ArrayList<Tuple>();
    private TransactionId lastTId = null;
    /**
     * Create a HeapPage from a set of bytes of data read from disk. The format
     * of a HeapPage is a set of header bytes indicating the slots of the page
     * that are in use, some number of tuple slots. Specifically, the number of
     * tuples is equal to:
     * <p>
     * floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
     * <p>
     * where tuple size is the size of tuples in this database table, which can
     * be determined via {@link Catalog#getTupleDesc}. The number of 8-bit
     * header words is equal to:
     * <p>
     * ceiling(no. tuple slots / 8)
     * <p>
     *
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#getPageSize()
     */
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pid = id;
        this.td = Database.getCatalog().getTupleDesc(id.getTableId());
        this.numSlots = getNumTuples();
        DataInputStream dis = new DataInputStream(
                new ByteArrayInputStream(data));

        // allocate and read the header slots of this page
        header = new byte[getHeaderSize()];
        for (int i = 0; i < header.length; i++)
            header[i] = dis.readByte();

        tuples = new Tuple[numSlots];
        try {
            // allocate and read the actual records of this page
            for (int i = 0; i < tuples.length; i++){
                tuples[i] = readNextTuple(dis, i);
            }             
        } catch (NoSuchElementException e) {
            e.printStackTrace();
        }
        dis.close();

        setBeforeImage();
    }

    /**
     * Retrieve the number of tuples on this page.
     *
     * @return the number of tuples on this page
     */
    private int getNumTuples() {
        // some code goes here
        // return 0;
        int tupleSize = this.td.getSize();
        int pageSize = BufferPool.getPageSize();
        double tuplesNum = Math.floor((pageSize * 8) / (tupleSize * 8 + 1));
        return (int) tuplesNum;
    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with
     * each tuple occupying tupleSize bytes
     *
     * @return the number of bytes in the header of a page in a HeapFile with
     * each tuple occupying tupleSize bytes
     */
    private int getHeaderSize() {

        // some code goes here
        // return 0;
        return (int) Math.ceil((float)getNumTuples() / 8);
    }

    /**
     * Return a view of this page before it was modified -- used by recovery
     */
    public HeapPage getBeforeImage() {
        try {
            byte[] oldDataRef = null;
            synchronized (oldDataLock) {
                oldDataRef = oldData;
            }
            return new HeapPage(pid, oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            // should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }

    public void setBeforeImage() {
        synchronized (oldDataLock) {
            oldData = getPageData().clone();
        }
    }

    /**
     * @return the PageId associated with this page.
     */
    public HeapPageId getId() {
        // some code goes here
        // throw new UnsupportedOperationException("implement this");
        return this.pid;
    }

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId)
            throws NoSuchElementException {
        // if associated bit is not set, read forward to the next tuple, and
        // return null.
        if (!isSlotUsed(slotId)) {
            for (int i = 0; i < td.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException(
                            "error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(td);
        RecordId rid = new RecordId(pid, slotId);
        t.setRecordId(rid);
        try {
            for (int j = 0; j < td.numFields(); j++) {
                Field f = td.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * Generates a byte array representing the contents of this page. Used to
     * serialize this page to disk.
     * <p>
     * The invariant here is that it should be possible to pass the byte array
     * generated by getPageData to the HeapPage constructor and have it produce
     * an identical HeapPage object.
     *
     * @return A byte array correspond to the bytes of this page.
     * @see #HeapPage
     */
    public byte[] getPageData() {
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // create the header of the page
        for (int i = 0; i < header.length; i++) {
            try {
                dos.writeByte(header[i]);
            } catch (IOException e) {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i = 0; i < tuples.length; i++) {

            // empty slot
            if (!isSlotUsed(i)) {
                for (int j = 0; j < td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot
            for (int j = 0; j < td.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zerolen = BufferPool.getPageSize()
                - (header.length + td.getSize() * tuples.length); // - numSlots
        // *
        // td.getSize();
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty
     * HeapPage. Used to add new, empty pages to the file. Passing the results
     * of this method to the HeapPage constructor will create a HeapPage with no
     * valid tuples in it.
     *
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.getPageSize();
        return new byte[len]; // all 0
    }

    /**
     * Delete the specified tuple from the page; the corresponding header bit
     * should be updated to reflect that it is no longer stored on any page.
     *
     * @param t The tuple to delete
     * @throws DbException if this tuple is not on this page, or tuple slot is already
     *                     empty.
     */
    public void deleteTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
    	RecordId rid = t.getRecordId();
    	if(!rid.getPageId().equals(pid)){
    		throw new DbException("tuple not on this page!");
    	}
    	if(!isSlotUsed(rid.getTupleNumber())){
    		throw new DbException("tuple slot is empty!");
    	}
    	markSlotUsed(rid.getTupleNumber(), false);
    	tuples[rid.getTupleNumber()] = null;
    }

    /**
     * Adds the specified tuple to the page; the tuple should be updated to
     * reflect that it is now stored on this page.
     *
     * @param t The tuple to add.
     * @throws DbException if the page is full (no empty slots) or tupledesc is
     *                     mismatch.
     */
    public void insertTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
    	TupleDesc tDesc = t.getTupleDesc();
    	if(!tDesc.equals(td)){
    		throw new DbException("tupledesc is mismatch!");
    	}
    	if(getNumEmptySlots() == 0){
    		throw new DbException("page is full!");
    	}
        for (int i = 0; i < tuples.length; i++){
            if(tuples[i] == null){
            	tuples[i] = t;
            	RecordId rid = new RecordId(pid, i);
            	t.setRecordId(rid);
            	break;
            }
        }  
    }

    /**
     * Marks this page as dirty/not dirty and record that transaction that did
     * the dirtying
     */
    public void markDirty(boolean dirty, TransactionId tid) {
        // some code goes here
        // not necessary for lab1

    	if(dirty){
    		lastTId = tid;
    	}else{
    		lastTId = null;
    	}
    	
    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null
     * if the page is not dirty
     */
    public TransactionId isDirty() {
        // some code goes here
        // Not necessary for lab1
        //return null;
    	return lastTId;
    }

    /**
     * Returns the number of empty slots on this page.
     */
    public int getNumEmptySlots() {
        // some code goes here
        // return 0;
        int sum = 0;
        for (int i = 0; i < tuples.length; ++i) {
            if (tuples[i] == null) {
                sum += 1;
            }
        }
        return sum;
    }

    /**
     * Returns true if associated slot on this page is filled.
     */
    public boolean isSlotUsed(int i) {
        // some code goes here
        //return false;
        if (i < 0) {
            return false;
        }

        String bits = byteToBit(header[i / 8]);

        int label = i % 8;
        if (bits.charAt(7 - label) == '1') {
            return true;

        }
        return false;
    }

    /**
     * Abstraction to fill or clear a slot on this page.
     */
    private void markSlotUsed(int i, boolean value) {
        // some code goes here
        // not necessary for lab1
    	char[] bits = byteToBit(header[i / 8]).toCharArray();
    	int label = i % 8;
    	if(value){
    		bits[7 - label] = '1';
    	}else{
    		bits[7 - label] = '0';
    	}
    	String result = new String(bits);
    	header[i / 8] = decodeBinaryString(result);
    }

    /**
     * @return an iterator over all tuples on this page (calling remove on this
     * iterator throws an UnsupportedOperationException) (note that this
     * iterator shouldn't return tuples in empty slots!)
     */
    public Iterator<Tuple> iterator() {
        // some code goes here
        //return null;
        validTuples.clear();
        int index = 0;
        for (int i = 0; i < tuples.length; ++i) {
            if (tuples[i] != null) {
                validTuples.add(index, tuples[i]);
                index++;
            }
        }
        return validTuples.iterator();

    }

    private String byteToBit(byte b) {
        return ""
                + (byte) ((b >> 7) & 0x1) + (byte) ((b >> 6) & 0x1)
                + (byte) ((b >> 5) & 0x1) + (byte) ((b >> 4) & 0x1)
                + (byte) ((b >> 3) & 0x1) + (byte) ((b >> 2) & 0x1)
                + (byte) ((b >> 1) & 0x1) + (byte) ((b >> 0) & 0x1);
    }
    
    private byte decodeBinaryString(String byteStr) {  
        int re, len;  
        if (null == byteStr) {  
            return 0;  
        }  
        len = byteStr.length();  
        if (len != 4 && len != 8) {  
            return 0;  
        }  
        if (len == 8) {
            if (byteStr.charAt(0) == '0') {
                re = Integer.parseInt(byteStr, 2);  
            } else {
                re = Integer.parseInt(byteStr, 2) - 256;  
            }  
        } else {
            re = Integer.parseInt(byteStr, 2);  
        }  
        return (byte) re;  
    }  
}
