package org.apache.jena.dboe.trans.bplustree;

import fr.gdd.sage.interfaces.RandomIterator;
import org.apache.jena.atlas.lib.InternalErrorException;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.atlas.lib.tuple.TupleMap;
import org.apache.jena.dboe.base.buffer.RecordBuffer;
import org.apache.jena.dboe.base.record.Record;
import org.apache.jena.tdb2.lib.TupleLib;
import org.apache.jena.tdb2.store.NodeId;

import java.util.*;


/**
 * This {@link RandomJenaIterator} enables random exploration of patterns.
 * This heavily depends on the {@link BPlusTree}
 * data structure, since it relies on {@link AccessPath} to
 * find out the boundary of the scan and draw a random element from
 * it.
 *
 * (TODO) This is an upcoming work.
 *
 * This is inspired from {@link BPTreeRangeIterator}.
 **/
public class RandomJenaIterator implements Iterator<Tuple<NodeId>>, RandomIterator {
    // Convert path to a stack of iterators
    private final Deque<Iterator<BPTreePage>> stack = new ArrayDeque<Iterator<BPTreePage>>();
    final private Record minRecord;
    final private Record maxRecord;
    // private Iterator<Tuple<NodeId>> current;
    private Iterator<Record> current; // current page
    private Tuple<NodeId> slot = null;
    private Tuple<NodeId> r = null;
    public boolean finished = false;

    // double iterator to get record. temporary until using mapper on
    // records instead of iteratormapper.
    // private Iterator<Tuple<Record>> currentRecord;
    private Record currentRecord;
    private TupleMap tupleMap;
    BPTreeNode root;

    private BPTreeRecords firstPage;



    // (TODO) probably an argument to state if it is the root or not
    public RandomJenaIterator(TupleMap tupleMap, BPTreeNode node, Record minRec, Record maxRec) {
        this.root = node;
        this.tupleMap = tupleMap;
        this.minRecord = minRec;
        this.maxRecord = maxRec;
        firstPage = loadStack(node, null);
        current = getRecordsIterator(firstPage, minRecord, maxRecord);
    }

    public Tuple<NodeId> getCurrentTupleId() {
        return r;
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
            return false;
        }

        currentRecord = current.next();
        slot = TupleLib.tuple(currentRecord, tupleMap);
        
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

        if (iter == null || !iter.hasNext()) return null;
        
        BPTreePage p = iter.next();
        BPTreeRecords r = null;
        if (p instanceof BPTreeNode) {
            r = loadStack((BPTreeNode) p, null);
        } else {
            r = (BPTreeRecords) p;
        }
        return getRecordsIterator(r, minRecord, maxRecord);
    }
    
    // ---- Places we touch blocks.

    private static Iterator<Record> getRecordsIterator(BPTreeRecords records,
                                                       Record minRecord, Record maxRecord) {
        records.bpTree.startReadBlkMgr();
        Iterator<Record> iter = records.getRecordBuffer().iterator(minRecord, maxRecord);
        records.bpTree.finishReadBlkMgr();
        return iter;
    }

    private BPTreeRecords loadStack(BPTreeNode node, Record from) {
        AccessPath path = new AccessPath(null);
        node.bpTree.startReadBlkMgr();

        if (from != null) { // for the resuming the preemptable iterator
            node.internalSearch(path, from);
        } else if (minRecord == null) {
            node.internalMinRecord(path);
        } else {
            node.internalSearch(path, minRecord);
        };
        
        var steps = path.getPath();
        
        for (AccessPath.AccessStep step_o : steps) {
            BPTreeNode n = step_o.node;
            
            Iterator<BPTreePage> it = null;
            // (TODO) better way to do that, i.e. `from` and
            // (TODO) `minRecord` seem more related than expected.
            if (from != null) {
                it = n.iterator(from, maxRecord);
            } else {
                it = n.iterator(minRecord, maxRecord);
            }
            if (it == null || !it.hasNext())
                continue;
            BPTreePage p = it.next();
            stack.push(it);
        }
        BPTreePage p = steps.get(steps.size() - 1).page;
        
        if (!(p instanceof BPTreeRecords))
            throw new InternalErrorException("Last path step not to a records block");
        
        node.bpTree.finishReadBlkMgr();
        return (BPTreeRecords) p;
    }

    @Override
    public Tuple<NodeId> next() {
        // (TODO) (TODO)
        if (!hasNext())
            throw new NoSuchElementException();

        this.r = slot;
        if (r == null)
            throw new InternalErrorException("Null slot after hasNext is true");
        // slotRecord = null;
        slot = null; // consumed
        // return r;
        random();
        // (TODO) (TODO)
        return null;
    }

    /**
     * Cardinality estimation exploiting the fact that the underlying
     * data structure is a balanced tree. When the number of results
     * is small, more precision is needed. Fortunately, this often
     * means that results are spread among one or two pages which
     * allows us to precisely count using binary search.
     *
     * (TODO) Take into account possible deletions.
     */
    @Override
    public long cardinality() {
        // (TODO) revamp the whole thing
        /*
        RandomJenaIterator ji = new RandomJenaIterator(tupleMap, root, minRecord, maxRecord);
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
        
        RecordBuffer recordBuffer = firstPage.getRecordBuffer();
        // (TODO) see `random()` for when min and max are actually found
        var min = recordBuffer.find(minRecord);
        var max = recordBuffer.find(maxRecord);
        
        sum += (min-max); // (max-min);
        */
        // (TODO) remove last iterator to get exact cardinality.

        // (TODO) if on more than 3 pages
        // (TODO) if on 2 pages firstPage + lastPage
        // (TODO) if on 1 page firstPage
        
        return -1;
    }

    /**
     * `random()` modifies the behavior of the iterator so that each
     * `next()` provides a new random binding within the
     * interval. Beware that it does not terminate nor it ensures
     * distinct bindings.
     *
     * As for `cardinality()`, it uses the underlying balanced tree to
     * efficiently reach a Record between two access paths.
     **/
    @Override
    public void random() {
        // (TODO) rework for it to work standalone now.
        AccessPath minPath = new AccessPath(null);
        root.internalSearch(minPath, minRecord);
        AccessPath maxPath = new AccessPath(null);
        root.internalSearch(maxPath, maxRecord);

        AccessPath randomPath = new AccessPath(null); // to build
        // #1 process common branch
        var minSteps = minPath.getPath();
        var maxSteps = maxPath.getPath();
        
        int i = 0;
        AccessPath.AccessStep minStep = minSteps.get(i);
        AccessPath.AccessStep maxStep = maxSteps.get(i);

        boolean done = false;
        while (i < minSteps.size() && i < maxSteps.size() && equalsStep(minStep, maxStep)) {
            randomPath.add(minStep.node, minStep.idx, minStep.page);
            ++i;
            if (i< minSteps.size() && i < maxSteps.size()) { // ugly
                minStep = minSteps.get(i);
                maxStep = maxSteps.get(i);
            } else {
                done = true;
            }
        }
        
        // done with common branch
        // #2 random between the remainder of the branches
        BPTreeNode lastMinNode = minStep.node; 
        BPTreeNode lastMaxNode = maxStep.node;

        // #A if last was same node but different pages
        if (!done && lastMinNode.getId() == lastMaxNode.getId()) {
            int idxMin  = minStep.idx;
            int idxMax  = maxStep.idx; 
            // (TODO) seeded random for the sake of reproducibility
            // (TODO) better casts
            int found_idx = (int) ((double)idxMin + Math.random() * ((double)(idxMax - idxMin)));
            
            BPTreePage chosenPage = lastMinNode.get(found_idx);
            randomPath.add(lastMinNode, found_idx, chosenPage);
            
            // #B random among the whole range of the underlying branch
            while (!(chosenPage instanceof BPTreeRecords)) {
                BPTreeNode chosenNode = (BPTreeNode) chosenPage;
                idxMax = chosenNode.getCount();
                found_idx = (int) (Math.random() * ((double)(idxMax)));
                chosenPage = ((BPTreeNode) chosenPage).get(found_idx);
                randomPath.add(chosenNode, found_idx, chosenPage);
            }
        }

        // #3 Randomize within the found page
        List<AccessPath.AccessStep> randomSteps = randomPath.getPath();
        AccessPath.AccessStep lastStep = randomSteps.get(randomSteps.size() - 1);
        BPTreePage p = lastStep.page;
        BPTreeRecords record = (BPTreeRecords) p;        
        RecordBuffer recordBuffer = record.getRecordBuffer();

        int min = recordBuffer.find(minRecord);
        int max = recordBuffer.find(maxRecord);
        
        min = min < 0 ? -min-1 : min;
        max = max < 0 ? -max-1 : max;

        if (min == max) {
            // No result for the random step, case closed.
            // finished = true;
            // finished = false; // (XXX)
            return ;
        } else {
            // Go on, proceed
            // finished = false;
        }

        int randomInRecords = ((int) ((double) min + Math.random()*((double) max - min)));
        Record currentRecord = recordBuffer._get(randomInRecords);
        
        stack.clear();
        firstPage = loadStack(root, currentRecord);
        this.current = getRecordsIterator(firstPage, currentRecord, maxRecord);
    }

    /**
     * Convenience function that checks the equality of two access paths.
     **/
    private static boolean equalsStep(AccessPath.AccessStep o1, AccessPath.AccessStep o2) {
        return o1.node.getId() == o2.node.getId() &&
            o1.idx == o2.idx &&
            o1.page.getId() == o2.page.getId();
    }

}
