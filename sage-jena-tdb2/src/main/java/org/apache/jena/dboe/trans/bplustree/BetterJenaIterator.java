package org.apache.jena.dboe.trans.bplustree;

import fr.gdd.sage.interfaces.BackendIterator;
import fr.gdd.sage.interfaces.RandomIterator;
import fr.gdd.sage.interfaces.SPOC;
import fr.gdd.sage.jena.SerializableRecord;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.atlas.lib.tuple.TupleMap;
import org.apache.jena.dboe.base.record.Record;
import org.apache.jena.dboe.base.record.RecordFactory;
import org.apache.jena.dboe.base.record.RecordMapper;
import org.apache.jena.tdb.lib.ColumnMap;
import org.apache.jena.tdb2.lib.TupleLib;
import org.apache.jena.tdb2.store.NodeId;
import org.apache.jena.util.iterator.NullIterator;
import org.apache.jena.util.iterator.SingletonIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Objects;


/**
 * This {@link BetterJenaIterator} enables pausing/resuming of scans, and
 * random exploration. This heavily depends on the {@link BPlusTree}
 * data structure. Indeed, it relies on {@link Record} to save the
 * cursor, and resume it later on; it relies on {@link AccessPath} to
 * find out the boundary of the scan and draw a random element from
 * it.
 *
 * It is heavily inspired by {@link BPTreeRangeIterator} and thus
 * could be aliased by `BPTreePreemptRangeIterator`.
 */
public class BetterJenaIterator implements BackendIterator<NodeId, SerializableRecord>, RandomIterator {
    static Logger log = LoggerFactory.getLogger(BetterJenaIterator.class);

    public boolean goRandom = false; // (TODO) possibly another dedicated iterator

    final BPlusTree tree;
    final Record min;
    final Record max;
    final RecordMapper mapper;
    final RecordFactory factory;
    final TupleMap tupleMap;

    Tuple<NodeId> current = null;
    Tuple<NodeId> previous = null;

    Iterator<Tuple<NodeId>> wrapped;



    public BetterJenaIterator(BPlusTree tree, Record min, Record max, RecordMapper mapper, RecordFactory factory, TupleMap tupleMap) {
        this.min = min;
        this.max = max;
        this.tree = tree;
        this.mapper = mapper;
        this.factory = factory;
        this.tupleMap = tupleMap;
        wrapped = tree.iterator(min, max, mapper);
    }

    /**
     * Singleton
     **/
    public BetterJenaIterator(Tuple<NodeId> pattern) {
        this.min = null;
        this.max = null;
        this.tree = null;
        this.mapper = null;
        this.factory = null;
        this.tupleMap = null;
        wrapped = new SingletonIterator<>(pattern);
    }

    public BetterJenaIterator() {
        this.min = null;
        this.max = null;
        this.tree = null;
        this.mapper = null;
        this.factory = null;
        this.tupleMap = null;
        wrapped = new NullIterator<>();
    }


    public Tuple<NodeId> getCurrentTuple() {
        return current;
    }

    @Override
    public void reset() {
        wrapped = tree.iterator(min, max, mapper);
    }

    @Override
    public NodeId getId(final int code) {
        if (current.len() > 3) {
            switch (code) {
            case SPOC.SUBJECT:
                return current.get(1);
            case SPOC.PREDICATE:
                return current.get(2);
            case SPOC.OBJECT:
                return current.get(3);
            case SPOC.CONTEXT:
                return current.get(0);
            }
        } else {
            switch (code) {
            case SPOC.SUBJECT:
                return current.get(0);
            case SPOC.PREDICATE:
                return current.get(1);
            case SPOC.OBJECT:
                return current.get(2);
            case SPOC.CONTEXT:
                return null;
            }
        }
        
        return null;
    }

    @Override
    public void skip(SerializableRecord to) {
        if (Objects.isNull(to) || Objects.isNull(to.record)) {
            // Corner case where an iterator indeed saved
            // its `previous()` but since this is the first
            // iteration, it is `null`. We still need to stay
            // at the beginning of the iterator.
            return;
        }

        // otherwise, we re-initialize the range iterator to
        // start at the key.
        wrapped = tree.iterator(to.record, max, mapper);

        // We are voluntarily one step behind with the saved
        // `Record`. Calling `hasNext()` and `next()` recover
        // a clean internal state.
        hasNext();
        next();
    }

    @Override
    public SerializableRecord current() {
        return Objects.isNull(current) ? null : new SerializableRecord(TupleLib.record(factory, current, tupleMap));
    }

    @Override
    public SerializableRecord previous() {
        return Objects.isNull(previous) ? null : new SerializableRecord(TupleLib.record(factory, previous, tupleMap));
    }
    
    @Override
    public boolean hasNext() {
        return wrapped.hasNext();
    }

    @Override
    public void next() {
        previous = current;
        current = wrapped.next();
    }
    


    // RandomIterator interface

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
        // (TODO) (TODO) (TODO)
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
        // (TODO) (TODO) (TODO)
    }

}
