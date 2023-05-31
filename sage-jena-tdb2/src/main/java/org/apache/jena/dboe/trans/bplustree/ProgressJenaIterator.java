package org.apache.jena.dboe.trans.bplustree;

import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.base.Sys;
import org.apache.jena.dboe.base.buffer.RecordBuffer;
import org.apache.jena.dboe.base.record.Record;
import org.apache.jena.tdb2.store.NodeId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * An iterator that allows measuring the estimated progress of execution, i.e.,
 * the number of explored elements over the estimated number to explore.
 */
public class ProgressJenaIterator {

    Logger log = LoggerFactory.getLogger(ProgressJenaIterator.class);

    /**
     * Number of walks to approximate how filled bptree's records are.
     */
    public static int NB_WALKS = 2; // (TODO) could be self-adaptive, and depend on the number of possible nodes between bounds

    long offset = 0; // the number of elements explored
    Long cardinality = null; // lazy loaded cardinality

    private final Record minRecord;
    private final Record maxRecord;

    private final BPTreeNode root;
    private final PreemptTupleIndexRecord ptir;

    public ProgressJenaIterator(PreemptTupleIndexRecord ptir, Record minRec, Record maxRec) {
        this.root = ptir.bpt.getNodeManager().getRead(ptir.bpt.getRootId());
        this.minRecord = Objects.isNull(minRec) ? root.minRecord() : minRec;
        this.maxRecord = Objects.isNull(maxRec) ? root.maxRecord() : maxRec;
        this.ptir = ptir;
    }

    /**
     * Empty iterator. Cardinality is zero.
     */
    public ProgressJenaIterator() {
        this.cardinality = 0L;
        this.maxRecord = null;
        this.minRecord = null;
        this.root = null;
        this.ptir = null;
    }

    /**
     * Singleton iterator. Cardinality is one.
     */
    public ProgressJenaIterator(PreemptTupleIndexRecord ptir, Tuple<NodeId> pattern) {
        this.cardinality = 1L;
        this.root = ptir.bpt.getNodeManager().getRead(ptir.bpt.getRootId());
        this.maxRecord = null;
        this.minRecord = null;
        this.ptir = null;
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
        if (Objects.isNull(cardinality)) {
            this.cardinality();
        }

        if (cardinality == 0) {
            return 1.0;
        } // already finished

        return ((double) this.offset) / (double) cardinality;
    }

    /**
     * Counts the number of elements by iterating over them. Warning: this is costly, but it provides exact
     * cardinality.
     * @return The exact number of elements in the iterator.
     */
    public long count() {
        if (Objects.isNull(ptir)) {
            return 0L;
        }
        Iterator<Tuple<NodeId>> wrapped = ptir.bpt.iterator(minRecord, maxRecord, ptir.getRecordMapper());
        long nbElements = 0;
        while (wrapped.hasNext()) {
            wrapped.next();
            nbElements += 1;
        }
        return nbElements;
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

        assert randomPath.getPath().size() == minPath.getPath().size();

        return randomPath;
    }

    /**
     * Estimates the cardinality of a triple/quad pattern knowing that
     * the underlying data structure is a balanced tree.
     * When the number of results is small, more precision is needed.
     * Fortunately, this often means that results are spread among one
     * or two pages, which allows us to precisely count using binary search.
     * <p>
     * (TODO) Take into account possible deletions.
     * (TODO) Triple patterns that return no solutions need to be handle elsewhere. Is it the case?
     *
     * @return An estimated cardinality.
     */
    public long cardinality(Integer... sample) {
        if (Objects.isNull(minRecord) && Objects.isNull(maxRecord) && Objects.isNull(root)) {
            return 0;
        }

        // number of random walks to estimate cardinalities in between boundaries.
        int nbWalks = Objects.isNull(sample) || sample.length == 0 ? NB_WALKS : sample[0];

        if (nbWalks == Integer.MAX_VALUE) {
            // MAX_VALUE goes for counting since it's the most costly, at least we want exact cardinality
            return count();
        }

        if (Objects.nonNull(this.cardinality)) {
            return cardinality; // already processed, lazy return.
        }

        AccessPath minPath = new AccessPath(null);
        AccessPath maxPath = new AccessPath(null);

        root.internalSearch(minPath, minRecord);
        root.internalSearch(maxPath, maxRecord);

        List<AccessPath.AccessStep> minSteps = minPath.getPath();
        List<AccessPath.AccessStep> maxSteps = maxPath.getPath();

        assert minSteps.size() == maxSteps.size();

        // estimating the size of B+tree's pages and nodes using random walks
        // we create 3 buckets of walks, since the border may heavily impact
        // the middle cardinality despite having significantly fewer elements.
        // This is a best effort solution, a better solution would be to use cardinalities
        // to weight collected cardinalities.
        double[] leftSizes = new double[minSteps.size() + 1];
        double[] leftCounts = new double[minSteps.size() + 1];
        double[] midCounts = new double[minSteps.size() + 1];
        double[] midSizes = new double[minSteps.size() + 1];
        double[] rightSizes = new double[minSteps.size() + 1];
        double[] rightCounts = new double[minSteps.size() + 1];

        log.debug("Performing {} random walks…", nbWalks);
        for (int i = 0; i < nbWalks; i++) {
            AccessPath path = randomWalk();
            int j = 0;
            for (; j < path.getPath().size(); j++) {
                // processing the nodes of the BPTree
                // if (path.getPath().get(j).node.id == minPath.getPath().get(j).node.id) { // left
                if (isParent(path, minPath, maxPath)) {
                    leftSizes[j] += path.getPath().get(j).node.getCount() + 1;
                    leftCounts[j] += 1;
                    //} else if (path.getPath().get(j).node.id == maxPath.getPath().get(j).node.id) { // right
                }  else if (isParent(path, maxPath, minPath)) {
                    rightSizes[j] += path.getPath().get(j).node.getCount() + 1;
                    rightCounts[j] += 1;
                } else { // mid
                    midSizes[j] += path.getPath().get(j).node.getCount() + 1;
                    midCounts[j] += 1;
                }
            }

            // processing the leaves (Records) of the BPTree
            // if (path.getPath().get(j - 1).node.id == minPath.getPath().get(j - 1).node.id) { // left
            if (isParent(path, minPath, maxPath)) {
                leftSizes[j] += path.getPath().get(j - 1).page.getCount();
                leftCounts[j] += 1;
                //} else if (path.getPath().get(j - 1).node.id == maxPath.getPath().get(j - 1).node.id) { // right
            } else if (isParent(path, maxPath, minPath)) {
                rightSizes[j] += path.getPath().get(j - 1).page.getCount();
                rightCounts[j] += 1;
            } else { // mid
                midSizes[j] += path.getPath().get(j - 1).page.getCount();
                midCounts[j] += 1;
            }

        }

        // processing a default value in case data is missing
        double[] avgSizes = new double[minSteps.size() + 1];
        for (int i = 0; i < rightSizes.length; ++i) {
            double count = (leftCounts[i] > 0 ? leftCounts[i] : 0) + (midCounts[i] > 0 ? midCounts[i] : 0) + (rightCounts[i] > 0 ? rightCounts[i] : 0);
            double value = (leftCounts[i] > 0 ? leftSizes[i] : 0) + (midCounts[i] > 0 ? midSizes[i] : 0) + (rightCounts[i] > 0 ? rightSizes[i] : 0);
            avgSizes[i] = value / count;
        }

        // processing the actual average per bucket, and filling the blanks with other buckets
        for (int i = 0; i < rightSizes.length; i++) {
            leftSizes[i] = leftCounts[i] > 0 ? leftSizes[i] / leftCounts[i] : avgSizes[i];
            midSizes[i] = midCounts[i] > 0 ? midSizes[i] / midCounts[i] : avgSizes[i];
            rightSizes[i] = rightCounts[i] > 0 ? rightSizes[i] / rightCounts[i] : avgSizes[i];
        }

        log.debug("LEFT  avg: {}", leftSizes);
        log.debug("MID   avg: {}", midSizes);
        log.debug("RIGHT avg: {}", rightSizes);


        // processing the cardinality using collected statistics
        double cardinality = 0;


        log.debug("min record " + minSteps);
        log.debug("max record " + maxSteps);

        for (int i = 0; i < minSteps.size(); i++) {
            AccessPath.AccessStep minStep = minSteps.get(i);
            AccessPath.AccessStep maxStep = maxSteps.get(i);

            if (!equalsStep(minStep, maxStep)) {
                double branchingFactorLeft = 1;
                double branchingFactorMid = 1;
                double branchingFactorRight = 1;

                for (int j = i + 1; j < rightSizes.length; j++) {
                    branchingFactorLeft *= leftSizes[j];
                    branchingFactorMid *= midSizes[j];
                    branchingFactorRight *= rightSizes[j];
                }

                //if (!minStep.node.isLeaf()) { // setup in average counts
                    // Why is it necessary? Probably due to {@link BPTreeNode} that
                    // have count = number of keys; while the number of pointers is
                    // count + 1
                    // branchingFactorLeft += leftSizes[leftSizes.length - 1];
                    // branchingFactorMid += midSizes[midSizes.length - 1];
                    // branchingFactorRight += rightSizes[rightSizes.length - 1];
                //}

                if (minStep.node.id == maxStep.node.id) {
                    // middle processed all at once
                    cardinality += (maxStep.idx - minStep.idx - 1) * branchingFactorMid;
                    if (minStep.idx != maxStep.idx) {
                        cardinality += branchingFactorLeft + branchingFactorRight;
                    }
                    // System.out.println("MID " + cardinality);
                } else {
                    // 1 node per side is left unexplored, we add its cardinality at the very end
                    // cardinality += (minStep.node.getCount() - minStep.idx + 1) * (branchingFactorLeft);
                    cardinality -= (minStep.idx) * branchingFactorLeft;
                    // System.out.println("LEFT IDX " + minStep.idx);
                    // System.out.println("LEFT CARD " + cardinality);
                    // cardinality += (maxStep.idx + 1) * (branchingFactorRight);

                    // System.out.println("RIGHT IDX " + maxStep.node.getCount() + " - " + maxStep.idx);
                    cardinality -= (maxStep.node.getCount() - maxStep.idx) * branchingFactorRight;
                    // System.out.println("RIGHT CARD " + cardinality);
                }
            }

            if (minStep.node.isLeaf()) {
                RecordBuffer minRecordBuffer = ((BPTreeRecords) minStep.page).getRecordBuffer();
                int idxMin = minRecordBuffer.find(minRecord);
                idxMin = idxMin < 0 ? -idxMin - 1 : idxMin;

                RecordBuffer maxRecordBuffer = ((BPTreeRecords) maxStep.page).getRecordBuffer();
                int idxMax = maxRecordBuffer.find(maxRecord);
                idxMax = idxMax < 0 ? -idxMax - 1 : idxMax;

                if (equalsStep(minStep, maxStep)) {
                    cardinality = idxMax - idxMin;
                } else {
                    // cardinality += minRecordBuffer.size() - idxMin;
                    // System.out.println("minSize " + maxRecordBuffer.size());
                    // System.out.println("idxMin " + idxMin);
                    cardinality -= idxMin;
                    // cardinality += idxMax;
                    // System.out.println("size " +  maxRecordBuffer.size() + " -  idxMax " + idxMax);
                    cardinality -= maxRecordBuffer.size() - idxMax;
                }
            }
        }

        this.cardinality = (long) cardinality;
        return (long) cardinality;
    }

    /**
     * Convenience function that checks the equality of two access paths.
     **/
    private static boolean equalsStep(AccessPath.AccessStep o1, AccessPath.AccessStep o2) {
        return o1.node.getId() == o2.node.getId() &&
                o1.idx == o2.idx &&
                o1.page.getId() == o2.page.getId();
    }


    private boolean isParent(AccessPath randomWalk, AccessPath base, AccessPath other) {
        for (int i = 0; i < base.getPath().size(); ++i) {
            if (base.getPath().get(i).node.id == other.getPath().get(i).node.id) {
                continue;
            }
            if (randomWalk.getPath().get(i).node.id == base.getPath().get(i).node.id) {
                return true;
            }
        }
        return false;
    }

}