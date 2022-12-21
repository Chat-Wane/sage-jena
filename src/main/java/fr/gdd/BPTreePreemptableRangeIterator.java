package fr.gdd;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.NoSuchElementException;

// import org.apache.jena.tdb.index.bplustree.*;
import org.apache.jena.dboe.trans.bplustree.*;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.atlas.lib.InternalErrorException;
// import org.apache.jena.tdb.base.record.Record;
import org.apache.jena.dboe.base.record.Record;
import org.apache.jena.dboe.base.buffer.RecordBuffer;
import org.apache.jena.dboe.trans.bplustree.AccessPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class BPTreePreemptableRangeIterator implements Iterator<Record> {
    static Logger log = LoggerFactory.getLogger(BPTreePreemptableRangeIterator.class);

    public static Iterator<Record> create(BPTreeNode node, Record minRec, Record maxRec) {
        if (minRec != null && maxRec != null && Record.keyGE(minRec, maxRec))
            return Iter.nullIter();
        return new BPTreePreemptableRangeIterator(node, minRec, maxRec);
    }

    // Convert path to a stack of iterators
    private final Deque<Iterator<BPTreePage>> stack = new ArrayDeque<Iterator<BPTreePage>>();
    final private Record minRecord;
    final private Record maxRecord;
    private Iterator<Record> current;
    private Record slot = null;
    private boolean finished = false;

    BPTreePreemptableRangeIterator(BPTreeNode node, Record minRec, Record maxRec) {
        this.minRecord = minRec;
        this.maxRecord = maxRec;
        BPTreeRecords r = loadStack(node);
        current = getRecordsIterator(r, minRecord, maxRecord);
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
        slot = current.next();
        return true;
    }
    
    // Move across the head of the stack until empty - then move next level.
    private Iterator<Record> moveOnCurrent() {
        Iterator<BPTreePage> iter = null;
        while (!stack.isEmpty()) {
            iter = stack.peek();
            if (iter.hasNext())
                break;
            stack.pop();
        }

        if (iter == null || !iter.hasNext())
            return null;
        BPTreePage p = iter.next();
        BPTreeRecords r = null;
        if (p instanceof BPTreeNode) {
            r = loadStack((BPTreeNode) p);
        } else {
            r = (BPTreeRecords) p;
        }
        return getRecordsIterator(r, minRecord, maxRecord);
    }
    
    // ---- Places we touch blocks.

    private static Iterator<Record> getRecordsIterator(BPTreeRecords records, Record minRecord, Record maxRecord) {
        // (TODO) move this to one time call
        // records.bpTree.startReadBlkMgr();
        Field bpTreeField = ReflectionUtils._getField(BPTreeRecords.class, "bpTree");
        Method startReadBlkMgrMethod = ReflectionUtils._getMethod(BPlusTree.class, "startReadBlkMgr");
        
        BPlusTree bpTree = ((BPlusTree) ReflectionUtils._callField(bpTreeField, records.getClass(), records));
        ReflectionUtils._callMethod(startReadBlkMgrMethod, bpTree.getClass(), bpTree);
        
        // Iterator<Record> iter = records.getRecordBuffer().iterator(minRecord, maxRecord);
        Method getRecordBufferMethod = ReflectionUtils._getMethod(BPTreeRecords.class, "getRecordBuffer");
        Iterator<Record> iter = ((RecordBuffer) ReflectionUtils._callMethod(getRecordBufferMethod, records.getClass(), records))
            .iterator(minRecord, maxRecord);
        
        // records.bpTree.finishReadBlkMgr();
        Method finishReadBlkMgrMethod = ReflectionUtils._getMethod(BPlusTree.class,"finishReadBlkMgr");
        ReflectionUtils._callMethod(finishReadBlkMgrMethod, bpTree.getClass(), bpTree);
        
        return iter;
    }

    private BPTreeRecords loadStack(BPTreeNode node) {
        AccessPath path = new AccessPath(null);

        // node.bpTree.startReadBlkMgr();
        // (TODO) save once and for all
        Field bpTreeField = ReflectionUtils._getField(BPTreeRecords.class, "bpTree");

        Method startReadBlkMgrMethod = ReflectionUtils._getMethod(BPlusTree.class, "startReadBlkMgr");
        
        BPlusTree bpTree = ((BPlusTree) ReflectionUtils._callField(bpTreeField, node.getClass(), node));
        ReflectionUtils._callMethod(startReadBlkMgrMethod, bpTree.getClass(), bpTree);

        if (minRecord == null) {
            // node.internalMinRecord(path);
            Method internalMinRecordMethod = ReflectionUtils._getMethod(BPlusTree.class, "internalMinRecord");
            ReflectionUtils._callMethod(internalMinRecordMethod, node.getClass(), node, path);
        } else {
            // node.internalSearch(path, minRecord);
            Method internalSearchMethod = ReflectionUtils._getMethod(BPlusTree.class, "internalSearch");
            ReflectionUtils._callMethod(internalSearchMethod, node.getClass(), node, path, minRecord);
        };
        
        var steps = path.getPath();
        // var AccessStepClass = AccessPath.class.getDeclaredClasses()[0];
        Class<?> AccessStepClass = ReflectionUtils._getClass("org.apache.jena.dboe.trans.bplustree.AccessPath$AccessStep");
        Field nodeField = ReflectionUtils._getField(AccessStepClass, "node");

        Method iteratorMethod = ReflectionUtils._getMethod(BPTreeNode.class,"iterator");
        
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
    public Record next() {
        if (!hasNext())
            throw new NoSuchElementException();
        Record r = slot;
        if (r == null)
            throw new InternalErrorException("Null slot after hasNext is true");
        slot = null;
        return r;
    }


    
}
