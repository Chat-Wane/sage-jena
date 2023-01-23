package fr.gdd.jena;

import fr.gdd.common.ReflectionUtils;
import fr.gdd.common.BackendIterator;
import fr.gdd.common.SPOC;
import fr.gdd.common.RandomIterator;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.jena.dboe.trans.bplustree.*;
import org.apache.jena.atlas.lib.InternalErrorException;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.atlas.lib.tuple.TupleMap;
import org.apache.jena.dboe.base.record.Record;
import org.apache.jena.dboe.base.buffer.RecordBuffer;
import org.apache.jena.dboe.trans.bplustree.AccessPath;
import org.apache.jena.tdb2.lib.TupleLib;
import org.apache.jena.tdb2.store.NodeId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class JenaIterator implements BackendIterator<NodeId, Record>, RandomIterator {
    static Logger log = LoggerFactory.getLogger(BPTreePreemptableRangeIterator.class);
    // Convert path to a stack of iterators
    private final Deque<Iterator<BPTreePage>> stack = new ArrayDeque<Iterator<BPTreePage>>();
    final private Record minRecord;
    final private Record maxRecord;
    // private Iterator<Tuple<NodeId>> current;
    private Iterator<Record> current; // current page
    private Tuple<NodeId> slot = null;
    private Tuple<NodeId> r = null;
    private boolean finished = false;

    // double iterator to get record. temporary until using mapper on
    // records instead of iteratormapper.
    // private Iterator<Tuple<Record>> currentRecord;
    private Record currentRecord;
    private Record previousRecord;
    private TupleMap tupleMap;
    BPTreeNode root;

    private BPTreeRecords firstPage;
    
    JenaIterator(TupleMap tupleMap, BPTreeNode node, Record minRec, Record maxRec) {
        this.root = node;
        this.tupleMap = tupleMap;
        this.minRecord = minRec;
        this.maxRecord = maxRec;
        firstPage = loadStack(node, null);
        current = getRecordsIterator(firstPage, minRecord, maxRecord);
    }

    private void end() {
        finished = true;
        current = null;
    }

    public void close() {
        System.out.println("CLOOOOZZZZ");
        if (!finished)
            end();
    }


    
    @Override
    public void reset() {
        stack.clear();
        previousRecord = null;
        currentRecord = null;
        firstPage = loadStack(root, null);
        //BPTreeRecords r = loadStack(root, null);
        current = getRecordsIterator(firstPage, minRecord, maxRecord);
    }

    @Override
    public NodeId getId(final int code) {
        if (r.len() > 3) {
            switch (code) {
            case SPOC.SUBJECT:
                return r.get(1);
            case SPOC.PREDICATE:
                return r.get(2);
            case SPOC.OBJECT:
                return r.get(3);
            case SPOC.CONTEXT:
                return r.get(0);
            }
        } else {
            switch (code) {
            case SPOC.SUBJECT:
                return r.get(0);
            case SPOC.PREDICATE:
                return r.get(1);
            case SPOC.OBJECT:
                return r.get(2);
            case SPOC.CONTEXT:
                return null;
            }
        }
        
        return null;
    }

    @Override
    public void skip(Record to) {
        if (to == null) {
            return;
        }
        stack.clear();
        BPTreeRecords r = loadStack(root, to);
        current = getRecordsIterator(r, to, maxRecord);
        hasNext(); // because it's on step behind with Record to
        next();
    }

    @Override
    public Record current() {
        return this.currentRecord;
    }

    @Override
    public Record previous() {
        return this.previousRecord;
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
        previousRecord = currentRecord;
        currentRecord = current.next();
        slot = TupleLib.tuple(currentRecord, tupleMap);

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
        System.out.printf("LOADING PAGE %s \n", p.getId());
        if (p instanceof BPTreeNode) {
            System.out.printf("LOAD A STACK: %s", p.getId());

            r = loadStack((BPTreeNode) p, null);
        } else {
            r = (BPTreeRecords) p;
        }
        return getRecordsIterator(r, minRecord, maxRecord);
    }
    
    // ---- Places we touch blocks.

    // private static Iterator<Tuple<NodeId>> getRecordsIterator(BPTreeRecords records,
    private static Iterator<Record> getRecordsIterator(BPTreeRecords records,
                                                       Record minRecord, Record maxRecord) {
        // (TODO) move this to one time call
        // records.bpTree.startReadBlkMgr();
        Field bpTreeField = ReflectionUtils._getField(BPTreePage.class, "bpTree");
        Method startReadBlkMgrMethod = ReflectionUtils._getMethod(BPlusTree.class, "startReadBlkMgr");
        
        BPlusTree bpTree = ((BPlusTree) ReflectionUtils._callField(bpTreeField, BPTreePage.class, records));
        ReflectionUtils._callMethod(startReadBlkMgrMethod, bpTree.getClass(), bpTree);
        
        // Iterator<Record> iter = records.getRecordBuffer().iterator(minRecord, maxRecord);
        Method getRecordBufferMethod = ReflectionUtils._getMethod(BPTreeRecords.class, "getRecordBuffer");
        RecordBuffer recordBuffer = (RecordBuffer) ReflectionUtils._callMethod(getRecordBufferMethod, records.getClass(), records);
        Iterator<Record> iter = recordBuffer.iterator(minRecord, maxRecord); //, mapper);

        // var min = recordBuffer.find(minRecord);
        // var max = recordBuffer.find(maxRecord);
        // System.out.printf("Size using recordbuffer %s-%s =  %s\n", max, min, max-min);
        
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

            // Field idxField = ReflectionUtils._getField(AccessStepClass, "idx");
            // int idx = (int) ReflectionUtils._callField(idxField, step.getClass(), step);
            // System.out.printf("step.idx : %s\n", idx);
            
            // Iterator<BPTreePage> it = n.iterator(minRecord, maxRecord);
            Iterator<BPTreePage> it = null;
            // (TODO) better way to do that, i.e. `from` and
            // (TODO) `minRecord` seem more related than expected.
            if (from != null) {
                it =  ((Iterator<BPTreePage>)
                       ReflectionUtils._callMethod(iteratorMethod, n.getClass(), n,
                                                   from, maxRecord));
            } else {
                it = ((Iterator<BPTreePage>)
                      ReflectionUtils._callMethod(iteratorMethod, n.getClass(), n,
                                                  minRecord, maxRecord));
                
            }
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

    @Override
    public void next() {
        try { // (TODO) remove this, only for testing timeout
            Thread.sleep(100);
        } catch (Exception e) {
        }
        
        if (!hasNext())
            throw new NoSuchElementException();

        this.r = slot;
        if (r == null)
            throw new InternalErrorException("Null slot after hasNext is true");
        // slotRecord = null;
        slot = null; // consumed
        // return r;
    }
    


    // RandomIterator interface
    
    @Override
    public long cardinality() {
        JenaIterator ji = new JenaIterator(tupleMap, root, minRecord, maxRecord);
        long sum = 0;
        
        for (Iterator<BPTreePage> ji_it : ji.getStack()) {
            while (ji_it.hasNext()) {
                BPTreePage node_or_record = ji_it.next();
                if (node_or_record == null) {
                    continue;
                }
            
                if (node_or_record != null && node_or_record instanceof BPTreeNode) {
                    BPTreeNode node = (BPTreeNode) node_or_record;
                    sum += node.getCount();
                    // (TODO) multiply by depth
                } else {
                    BPTreeRecords records = (BPTreeRecords) node_or_record;
                    sum += records.getCount();
                }
            }
        };
        
        Method getRecordBufferMethod = ReflectionUtils._getMethod(BPTreeRecords.class, "getRecordBuffer");
        RecordBuffer recordBuffer = (RecordBuffer) ReflectionUtils._callMethod(getRecordBufferMethod, firstPage.getClass(), firstPage);
        
        var min = recordBuffer.find(minRecord);
        var max = recordBuffer.find(maxRecord);
        
        sum += (min-max); // (max-min);

        // (TODO) remove last iterator to get exact cardinality.

        // (TODO) if on more than 3 pages
        // (TODO) if on 2 pages firstPage + lastPage
        // (TODO) if on 1 page firstPage
        
        return sum;
    }

    @Override
    public void random() {
        // (TODO)
        Method internalSearchMethod = ReflectionUtils._getMethod(BPTreeNode.class, "internalSearch", AccessPath.class, Record.class);
        AccessPath minPath = new AccessPath(null);
        ReflectionUtils._callMethod(internalSearchMethod, root.getClass(), root, minPath, minRecord);
        
        AccessPath maxPath = new AccessPath(null);
        ReflectionUtils._callMethod(internalSearchMethod, root.getClass(), root, maxPath, maxRecord);
        // System.out.printf("MIN PATH %s\n", minPath.toString()); 
        // System.out.printf("MAX PATH %s\n", maxPath.toString());

        AccessPath randomPath = new AccessPath(null);
        // #1 process common branch
        var minSteps = minPath.getPath();
        var maxSteps = maxPath.getPath();

        Class<?> AccessStepClass = ReflectionUtils._getClass("org.apache.jena.dboe.trans.bplustree.AccessPath$AccessStep");
        Field nodeField = ReflectionUtils._getField(AccessStepClass, "node");
        Field idxField  = ReflectionUtils._getField(AccessStepClass, "idx");
        Field pageField = ReflectionUtils._getField(AccessStepClass, "page");
        
        int i = 0;
        var minStep = AccessStepClass.cast(minSteps.get(i));
        var maxStep = AccessStepClass.cast(maxSteps.get(i));
        // (TODO) better equal comparison
        boolean done = false;
        while (i < minSteps.size() && i < maxSteps.size() && equalsStep(minStep, maxStep)) {
            BPTreeNode node = ((BPTreeNode) ReflectionUtils._callField(nodeField, minStep.getClass(), minStep));
            int        idx  = ((int)        ReflectionUtils._callField(idxField , minStep.getClass(), minStep));
            BPTreePage page = ((BPTreePage) ReflectionUtils._callField(pageField, minStep.getClass(), minStep));

            randomPath.add(node, idx, page);
            ++i;
            if (i< minSteps.size() && i < maxSteps.size()) { // ugly
                minStep = AccessStepClass.cast(minSteps.get(i));
                maxStep = AccessStepClass.cast(maxSteps.get(i));
            } else {
                done = true;
            }
        }
        
        // System.out.printf("After common %s\n", randomPath);
        // #2 random between the remainder of the branches
        BPTreeNode lastMinNode = ((BPTreeNode) ReflectionUtils._callField(nodeField, minStep.getClass(), minStep));
        BPTreeNode lastMaxNode = ((BPTreeNode) ReflectionUtils._callField(nodeField, minStep.getClass(), maxStep));
        // System.out.printf("%s\n", lastNode.getCount());

        // #A if last was same node but different pages
        if (!done && lastMinNode.getId() == lastMaxNode.getId()) {
            int idxMin  = ((int) ReflectionUtils._callField(idxField , minStep.getClass(), minStep));
            int idxMax  = ((int) ReflectionUtils._callField(idxField , maxStep.getClass(), maxStep));
            // (TODO) seeded random for the sake of reproducibility
            // (TODO) better casts
            int found_idx = (int) ((double)idxMin + Math.random() * ((double)(idxMax - idxMin)));
            
            Method getMethod = ReflectionUtils._getMethod(BPTreeNode.class, "get", int.class);

            BPTreePage chosenPage = (BPTreePage) ReflectionUtils._callMethod(getMethod, BPTreeNode.class, lastMinNode, found_idx);
            // System.out.printf("FOUND RANDOM %s\n", found_idx );
            // System.out.printf("Page %s\n", chosenPage.getId());
            randomPath.add(lastMinNode, found_idx, chosenPage);
            
            // System.out.printf("After common node %s\n", randomPath);
            
            // #B random among the whole range of the underlying branch
            // 
            // while (!chosenNode.isLeaf()) {
            while (!(chosenPage instanceof BPTreeRecords)) {
                BPTreeNode chosenNode = (BPTreeNode) chosenPage;
                idxMax = chosenNode.getCount();
                found_idx = (int) (Math.random() * ((double)(idxMax)));
                // System.out.printf("FOUND RANDOM %s\n", found_idx );
                chosenPage = (BPTreePage) ReflectionUtils._callMethod(getMethod, BPTreeNode.class, chosenPage, found_idx);
                randomPath.add(chosenNode, found_idx, chosenPage);
            }

            // System.out.printf("After going down %s\n", randomPath);
        }

        var randomSteps = randomPath.getPath();
        var lastStep = AccessStepClass.cast(randomSteps.get(randomSteps.size() - 1));
        BPTreePage p = (BPTreePage) ReflectionUtils._callField(pageField, lastStep.getClass(), lastStep);

        BPTreeRecords record = (BPTreeRecords) p;

        Method getRecordBufferMethod = ReflectionUtils._getMethod(BPTreeRecords.class, "getRecordBuffer");
        RecordBuffer recordBuffer = (RecordBuffer) ReflectionUtils._callMethod(getRecordBufferMethod, record.getClass(), record);

        var min = -recordBuffer.find(minRecord)-1; // must be negative -1 
        var max = -recordBuffer.find(maxRecord)-1;
        // System.out.printf("min %s   ;  max %s\n", min, max);

        int randomInRecords = ((int) ((double) min + Math.random()*((double) max - min)));
        Record currentRecord = recordBuffer._get(randomInRecords);

        stack.clear();
        firstPage = loadStack(root, currentRecord);
        this.current = getRecordsIterator(firstPage, currentRecord, maxRecord);
    }

    
    public Deque<Iterator<BPTreePage>> getStack() {
        return stack;
    }

    public Iterator<Record> getCurrentIterator() {
        return current;
    }

    private static boolean equalsStep(Object o1, Object o2) {
        Class<?> AccessStepClass = ReflectionUtils._getClass("org.apache.jena.dboe.trans.bplustree.AccessPath$AccessStep");
        Field nodeField = ReflectionUtils._getField(AccessStepClass, "node");
        Field idxField  = ReflectionUtils._getField(AccessStepClass, "idx");
        Field pageField = ReflectionUtils._getField(AccessStepClass, "page");
        var o1_casted = AccessStepClass.cast(o1);
        var o2_casted = AccessStepClass.cast(o2);

        BPTreeNode node_o1 = ((BPTreeNode) ReflectionUtils._callField(nodeField, o1_casted.getClass(), o1_casted));
        int        idx_o1  = ((int)        ReflectionUtils._callField(idxField , o1_casted.getClass(), o1_casted));
        BPTreePage page_o1 = ((BPTreePage) ReflectionUtils._callField(pageField, o1_casted.getClass(), o1_casted));

        BPTreeNode node_o2 = ((BPTreeNode) ReflectionUtils._callField(nodeField, o2_casted.getClass(), o2_casted));
        int        idx_o2  = ((int)        ReflectionUtils._callField(idxField , o2_casted.getClass(), o2_casted));
        BPTreePage page_o2 = ((BPTreePage) ReflectionUtils._callField(pageField, o2_casted.getClass(), o2_casted));

        return node_o1.getId() == node_o2.getId() && idx_o1 == idx_o2 && page_o1.getId() == page_o2.getId();
    }

}
