package org.apache.jena.dboe.trans.bplustree;

import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.dboe.base.buffer.RecordBuffer;
import org.apache.jena.dboe.base.record.Record;
import org.apache.jena.tdb2.store.NodeId;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * An iterator that allows measuring the estimated progress of execution, i.e.,
 * the number of explored elements over the estimated number to explore.
 */
public class ProgressJenaIterator {

    /**
     * Number of walks to approximate how filled bptree's records are.
     */
    private static final int NB_WALKS = 2;

    long offset = 0; // the number of elements explored
    Long cardinality = null; // lazy loaded cardinality

    private final Record minRecord;
    private final Record maxRecord;

    private final BPTreeNode root;

    public ProgressJenaIterator(PreemptTupleIndexRecord ptir, Record minRec, Record maxRec) {
        this.root = ptir.bpt.getNodeManager().getRead(ptir.bpt.getRootId());
        this.minRecord = Objects.isNull(minRec) ? root.minRecord() : minRec;
        this.maxRecord = Objects.isNull(maxRec) ? root.maxRecord() : maxRec;
    }

    /**
     * Empty iterator. Cardinality is zero.
     */
    public ProgressJenaIterator() {
        this.cardinality = 0L;
        this.maxRecord = null;
        this.minRecord = null;
        this.root = null;
    }

    /**
     * Singleton iterator. Cardinality is one.
     */
    public ProgressJenaIterator(PreemptTupleIndexRecord ptir, Tuple<NodeId> pattern) {
        this.cardinality = 1L;
        this.root = ptir.bpt.getNodeManager().getRead(ptir.bpt.getRootId());
        this.maxRecord = null;
        this.minRecord = null;
    }

    public void next() {
        this.offset += 1;
    }

    public long getOffset() {
        return offset;
    }

    public Serializable current() {
        return offset;
    }

    public Serializable previous() {
        return offset - 1;
    }

    public void skip(Serializable to) {
        this.offset = (Long) to;
    }

    public double getProgress() {
        if (Objects.isNull(cardinality)) { this.cardinality(); }

        if (cardinality == 0) { return 1.0; } // already finished

        return ((double)this.offset) / (double) cardinality;
    }

    /**
     * Performs random steps from the root to a leaf of a B+tree index.
     *
     * @return An `AccessPath` built using random steps.
     */
    protected AccessPath randomWalk() {
        AccessPath minPath = new AccessPath(null);
        AccessPath maxPath = new AccessPath(null);

        root.internalSearch(minPath, minRecord);
        root.internalSearch(maxPath, maxRecord);

        assert minPath.getPath().size() == maxPath.getPath().size();

        AccessPath randomPath = new AccessPath(null);

        AccessPath.AccessStep minStep = minPath.getPath().get(0);
        AccessPath.AccessStep maxStep = maxPath.getPath().get(0);

        int idxRnd = (int) (minStep.idx + Math.random() * (maxStep.idx - minStep.idx + 1));
        randomPath.add(minStep.node, idxRnd, minStep.node.get(idxRnd));
        AccessPath.AccessStep lastStep = randomPath.getPath().get(randomPath.getPath().size() - 1);

        while (!lastStep.node.isLeaf()) {
            BPTreeNode node = (BPTreeNode) lastStep.page;

            int idxMin = node.findSlot(minRecord);
            int idxMax = node.findSlot(maxRecord);

            idxMin = idxMin < 0 ? -idxMin - 1 : idxMin;
            idxMax = idxMax < 0 ? -idxMax - 1 : idxMax;

            idxRnd = (int) (idxMin + Math.random() * (idxMax - idxMin + 1));
            randomPath.add(node, idxRnd, node.get(idxRnd));
            lastStep = randomPath.getPath().get(randomPath.getPath().size() - 1);
        }

        // System.out.println("randomPath: " + randomPath);

        assert randomPath.getPath().size() == minPath.getPath().size();

        return randomPath;
    }

    /**
     * Estimates the cardinality of a triple/quad pattern knowing that
     * the underlying data structure is a balanced tree.
     * When the number of results is small, more precision is needed.
     * Fortunately, this often means that results are spread among one
     * or two pages, which allows us to precisely count using binary search.
     *
     * (TODO) Take into account possible deletions.
     * (TODO) Triple patterns that return no solutions need to be handle elsewhere. Is it the case?
     *
     * @return An estimated cardinality.
     */
    public long cardinality() {
        if (Objects.isNull(minRecord) && Objects.isNull(maxRecord) && Objects.isNull(root)) {
            return 0;
        }

        if (Objects.nonNull(this.cardinality)) {
            return cardinality; // already processed
        }

        AccessPath minPath = new AccessPath(null);
        AccessPath maxPath = new AccessPath(null);

        root.internalSearch(minPath, minRecord);
        root.internalSearch(maxPath, maxRecord);

        List<AccessPath.AccessStep> minSteps = minPath.getPath();
        List<AccessPath.AccessStep> maxSteps = maxPath.getPath();

        assert minSteps.size() == maxSteps.size();

        long[] pageSize = new long[minSteps.size() + 1];

        // estimating the size of B+tree's pages and nodes using random walks
        for (int i = 0; i < NB_WALKS; i++) {
            AccessPath path = randomWalk();
            int j = 0;
            for (; j < path.getPath().size(); j++) {
                pageSize[j] += path.getPath().get(j).node.getCount();
            }
            pageSize[j] += path.getPath().get(j - 1).page.getCount();
        }
        for (int i = 0; i < pageSize.length; i++) {
            pageSize[i] /= NB_WALKS;
        }

        long cardinality = 0;

        for (int i = 0; i < minSteps.size(); i++) {
            AccessPath.AccessStep minStep = minSteps.get(i);
            AccessPath.AccessStep maxStep = maxSteps.get(i);

            if (!equalsStep(minStep, maxStep)) {
                long branchingFactor = 1;
                for (int j = i + 1; j < pageSize.length; j++) {
                    branchingFactor *= pageSize[j];
                }
                if (!minStep.node.isLeaf()) {
                    // Why is it necessary? Probably due to {@link BPTreeNode} that
                    // have count = number of keys; while the number of pointers is
                    // count + 1
                    branchingFactor += pageSize[pageSize.length - 1];
                }

                if (minStep.node.id == maxStep.node.id) {
                    cardinality += (maxStep.idx - minStep.idx - 1) * branchingFactor;
                } else {
                    cardinality += (minStep.node.getCount() - minStep.idx) * branchingFactor;
                    cardinality += maxStep.idx * branchingFactor;
                }
            }

            if (minStep.node.isLeaf()) {
                RecordBuffer minRecordBuffer = ((BPTreeRecords) minStep.page).getRecordBuffer();
                int idxMin = minRecordBuffer.find(minRecord);
                idxMin = idxMin < 0 ? -idxMin - 1 : idxMin;

                RecordBuffer maxRecordBuffer = ((BPTreeRecords) maxStep.page).getRecordBuffer();
                int idxMax =  maxRecordBuffer.find(maxRecord);
                idxMax = idxMax < 0 ? -idxMax - 1 : idxMax;

                if (equalsStep(minStep, maxStep)) {
                    cardinality = idxMax - idxMin;
                } else {
                    cardinality += minRecordBuffer.size() - idxMin;
                    cardinality += idxMax;
                }
            }
        }

        this.cardinality = cardinality;
        return cardinality;
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
