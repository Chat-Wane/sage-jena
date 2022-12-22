package fr.gdd;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.jena.dboe.trans.bplustree.*;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.atlas.lib.InternalErrorException;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.atlas.lib.tuple.TupleMap;
import org.apache.jena.dboe.base.record.Record;
import org.apache.jena.dboe.base.record.RecordMapper;
import org.apache.jena.dboe.base.buffer.RecordBuffer;
import org.apache.jena.dboe.trans.bplustree.AccessPath;
import org.apache.jena.tdb2.lib.TupleLib;
import org.apache.jena.tdb2.store.NodeId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class BPTreePreemptableRangeIterator implements Iterator<Tuple<NodeId>> {
    static Logger log = LoggerFactory.getLogger(BPTreePreemptableRangeIterator.class);

    public static Iterator<Tuple<NodeId>> create(TupleMap tupleMap,
                                                 BPTreeNode node, Record minRec, Record maxRec,
                                                 RecordMapper<Tuple<NodeId>> mapper) {
        if (minRec != null && maxRec != null && Record.keyGE(minRec, maxRec))
            return Iter.nullIter();
        return new BPTreePreemptableRangeIterator(tupleMap, node, minRec, maxRec, mapper);
    }

    // Convert path to a stack of iterators
    private final Deque<Iterator<BPTreePage>> stack = new ArrayDeque<Iterator<BPTreePage>>();
    final private Record minRecord;
    final private Record maxRecord;
    final private RecordMapper<Tuple<NodeId>> mapper;
    // private Iterator<Tuple<NodeId>> current;
    private Iterator<Record> current;
    private Tuple<NodeId> slot = null;
    private boolean finished = false;

    // double iterator to get record. temporary until using mapper on
    // records instead of iteratormapper.
    // private Iterator<Tuple<Record>> currentRecord;
    private Record slotRecord;
    private TupleMap tupleMap;
    BPTreeNode root;

    
    
    BPTreePreemptableRangeIterator(TupleMap tupleMap, BPTreeNode node, Record minRec, Record maxRec, RecordMapper<Tuple<NodeId>> mapper) {
        this.root = node;
        this.tupleMap = tupleMap;
        this.minRecord = minRec;
        this.maxRecord = maxRec;
        BPTreeRecords r = loadStack(node, null);
        this.mapper = mapper;
        current = getRecordsIterator(r, minRecord, maxRecord, mapper);
    }


    public void skip(Record to) {
        stack.clear();
        BPTreeRecords r = loadStack(this.root, to);
        current = getRecordsIterator(r, to, maxRecord, mapper);
        next(); // because it's on step behind with Record to
    }

    public Record current() {
        return this.slotRecord;
    }
    
    
    @Override
    public boolean hasNext() {
        if (finished)
            return false;
        if (slot != null)
            return true;
        while (current != null && !current.hasNext()) {
            current = moveOnCurrent();
        }
        if (current == null) {
            end();
            return false;
        }

        // slot = current.next();
        slotRecord = current.next();
        slot = TupleLib.tuple(slotRecord, tupleMap);

        // <https://github.com/apache/jena/blob/31dc0d328c4858401e5d3fa99702c97eba0383a0/jena-db/jena-dboe-base/src/main/java/org/apache/jena/dboe/base/buffer/RecordBufferIteratorMapper.java#L77>
        //  maybe slot = rBuff.access(nextIdx, keySlot, mapper);
        return true;
    }
    
    // Move across the head of the stack until empty - then move next level.
    // private Iterator<Tuple<NodeId>> moveOnCurrent() {
    private Iterator<Record> moveOnCurrent() {
        Iterator<BPTreePage> iter = null;
        while (!stack.isEmpty()) {
            iter = stack.peek();
            if (iter.hasNext())
                break;
            stack.pop();
        }

        if (iter == null || !iter.hasNext()) return null;
        
        BPTreePage p = iter.next();
        BPTreeRecords r = null;
        if (p instanceof BPTreeNode) {
            r = loadStack((BPTreeNode) p, null);
        } else {
            r = (BPTreeRecords) p;
        }
        return getRecordsIterator(r, minRecord, maxRecord, mapper);
    }
    
    // ---- Places we touch blocks.

    // private static Iterator<Tuple<NodeId>> getRecordsIterator(BPTreeRecords records,
    private static Iterator<Record> getRecordsIterator(BPTreeRecords records,
                                                       Record minRecord, Record maxRecord,
                                                       RecordMapper<Tuple<NodeId>> mapper) {
        // (TODO) move this to one time call
        // records.bpTree.startReadBlkMgr();
        Field bpTreeField = ReflectionUtils._getField(BPTreePage.class, "bpTree");
        Method startReadBlkMgrMethod = ReflectionUtils._getMethod(BPlusTree.class, "startReadBlkMgr");
        
        BPlusTree bpTree = ((BPlusTree) ReflectionUtils._callField(bpTreeField, BPTreePage.class, records));
        ReflectionUtils._callMethod(startReadBlkMgrMethod, bpTree.getClass(), bpTree);
        
        // Iterator<Record> iter = records.getRecordBuffer().iterator(minRecord, maxRecord);
        Method getRecordBufferMethod = ReflectionUtils._getMethod(BPTreeRecords.class, "getRecordBuffer");
        Iterator<Record> iter = ((RecordBuffer) ReflectionUtils._callMethod(getRecordBufferMethod, records.getClass(), records))
            .iterator(minRecord, maxRecord); //, mapper);
        
        // records.bpTree.finishReadBlkMgr();
        Method finishReadBlkMgrMethod = ReflectionUtils._getMethod(BPlusTree.class,"finishReadBlkMgr");
        ReflectionUtils._callMethod(finishReadBlkMgrMethod, bpTree.getClass(), bpTree);
        return iter;
    }

    private BPTreeRecords loadStack(BPTreeNode node, Record from) {
        AccessPath path = new AccessPath(null);

        // node.bpTree.startReadBlkMgr();
        // (TODO) save once and for all
        Field bpTreeField = ReflectionUtils._getField(BPTreePage.class, "bpTree");

        Method startReadBlkMgrMethod = ReflectionUtils._getMethod(BPlusTree.class, "startReadBlkMgr");
        
        BPlusTree bpTree = ((BPlusTree) ReflectionUtils._callField(bpTreeField, BPTreePage.class, node));
        ReflectionUtils._callMethod(startReadBlkMgrMethod, bpTree.getClass(), bpTree);

        if (from != null) { // for the resuming the preemptable iterator
            // node.internalSearch(path, from);
            Method internalSearchMethod = ReflectionUtils._getMethod(BPTreeNode.class, "internalSearch", AccessPath.class, Record.class);
            ReflectionUtils._callMethod(internalSearchMethod, node.getClass(), node, path, from);
            

        } else if (minRecord == null) {
            // node.internalMinRecord(path);
            Method internalMinRecordMethod = ReflectionUtils._getMethod(BPTreeNode.class, "internalMinRecord", AccessPath.class);
            ReflectionUtils._callMethod(internalMinRecordMethod, node.getClass(), node, path);
        } else {
            // node.internalSearch(path, minRecord);
            Method internalSearchMethod = ReflectionUtils._getMethod(BPTreeNode.class, "internalSearch", AccessPath.class, Record.class);
            ReflectionUtils._callMethod(internalSearchMethod, node.getClass(), node, path, minRecord);
        };
        
        var steps = path.getPath();
        
        Class<?> AccessStepClass = ReflectionUtils._getClass("org.apache.jena.dboe.trans.bplustree.AccessPath$AccessStep");
        Field nodeField = ReflectionUtils._getField(AccessStepClass, "node");

        Method iteratorMethod = ReflectionUtils._getMethod(BPTreeNode.class, "iterator", Record.class, Record.class);
        
        for (Object step_o : steps) {
            // BPTreeNode n = step.node;
            var step = AccessStepClass.cast(step_o);
            BPTreeNode n = ((BPTreeNode) ReflectionUtils._callField(nodeField, step.getClass(), step));

            // Iterator<BPTreePage> it = n.iterator(minRecord, maxRecord);
            Iterator<BPTreePage> it = ((Iterator<BPTreePage>)
                                       ReflectionUtils._callMethod(iteratorMethod, n.getClass(), n,
                                                                   minRecord, maxRecord));
            if (it == null || !it.hasNext())
                continue;
            BPTreePage p = it.next();
            stack.push(it);
        }
        //        BPTreePage p = steps.get(steps.size() - 1).page;
        var lastStep = AccessStepClass.cast(steps.get(steps.size() - 1));
        Field pageField = ReflectionUtils._getField(AccessStepClass, "page");

        BPTreePage p = (BPTreePage) ReflectionUtils._callField(pageField, lastStep.getClass(), lastStep);
        
        if (!(p instanceof BPTreeRecords))
            throw new InternalErrorException("Last path step not to a records block");
        
        // node.bpTree.finishReadBlkMgr();
        Method finishReadBlkMgrMethod = ReflectionUtils._getMethod(BPlusTree.class, "finishReadBlkMgr");
        ReflectionUtils._callMethod(finishReadBlkMgrMethod, bpTree.getClass(), bpTree);

        return (BPTreeRecords) p;
    }

    // ----

    private void end() {
        finished = true;
        current = null;
    }

    // ----

    public void close() {
        if (!finished)
            end();
    }

    @Override
    public Tuple<NodeId> next() {
        if (!hasNext())
            throw new NoSuchElementException();

        Tuple<NodeId> r = slot;
        if (r == null)
            throw new InternalErrorException("Null slot after hasNext is true");
        // slotRecord = null;
        slot = null;
        return r;
    }
    
}
