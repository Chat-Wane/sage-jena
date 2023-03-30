package org.apache.jena.dboe.trans.bplustree;

import fr.gdd.sage.interfaces.BackendIterator;
import fr.gdd.sage.interfaces.SPOC;
import fr.gdd.sage.jena.SerializableRecord;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.atlas.lib.tuple.TupleMap;
import org.apache.jena.dboe.base.record.Record;
import org.apache.jena.dboe.base.record.RecordFactory;
import org.apache.jena.dboe.base.record.RecordMapper;
import org.apache.jena.tdb2.lib.TupleLib;
import org.apache.jena.tdb2.store.NodeId;
import org.apache.jena.util.iterator.NullIterator;
import org.apache.jena.util.iterator.SingletonIterator;

import java.util.Iterator;
import java.util.Objects;


/**
 * This {@link PreemptJenaIterator} enables pausing/resuming of scans.
 * It relies on {@link Record} to save the
 * cursor, and resume it later on; it relies on {@link AccessPath} to
 * find out the boundary of the scan and draw a random element from
 * it.
 **/
public class PreemptJenaIterator implements BackendIterator<NodeId, SerializableRecord> {
    final BPlusTree tree;
    Record min;
    Record max;
    final RecordMapper<Tuple<NodeId>> mapper;
    final RecordFactory factory;
    final TupleMap tupleMap;

    Tuple<NodeId> current = null;
    Tuple<NodeId> previous = null;

    Iterator<Tuple<NodeId>> wrapped;



    public PreemptJenaIterator(BPlusTree tree, Record min, Record max,
                               RecordMapper<Tuple<NodeId>> mapper, RecordFactory factory, TupleMap tupleMap) {
        this.min = min;
        this.max = max;
        this.tree = tree;
        this.mapper = mapper;
        this.factory = factory;
        this.tupleMap = tupleMap;
        wrapped = tree.iterator(min, max, mapper);
    }

    /**
     * Null Iterator, does not need any preemptive behavior.
     **/
    public PreemptJenaIterator() {
        this.min = null;
        this.max = null;
        this.tree = null;
        this.mapper = null;
        this.factory = null;
        this.tupleMap = null;
        this.wrapped = new NullIterator<>();
    }

    public boolean isNullIterator() {
        return this.wrapped instanceof NullIterator;
    }

    /**
     * Singleton Iterator, still need basic parameters since they are
     * needed for `previous()`/`current()` and `skip(to)` as well.
     */
    public PreemptJenaIterator(Tuple<NodeId> pattern, BPlusTree tree, Record min, Record max,
                               RecordMapper<Tuple<NodeId>> mapper, RecordFactory factory, TupleMap tupleMap) {
        this.min = min;
        this.max = max;
        this.tree = tree;
        this.mapper = mapper;
        this.factory = factory;
        this.tupleMap = tupleMap;
        wrapped = new SingletonIterator<>(pattern);
    }

    public boolean isSingletonIterator() {
        return this.wrapped instanceof SingletonIterator;
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

        if (isSingletonIterator()) { // you already worked or you are null and returned just before
            // called on previous() therefore `null` therefore produce the pattern again, or;
            // called on current() therefore `null` if not produced then produce the pattern, or;
            // called on current() therefore `record` if produced then do not produce again.
            hasNext();
            next();
            return;
        }

        // otherwise, we re-initialize the range iterator to
        // start at the key.
        if (Objects.isNull(tree)) {
            System.out.println("TREE NULL ? ");
            System.out.println("record " + to.record.toString());
            System.out.println("is Singleton? " + isSingletonIterator());
            System.out.println("is Null ? " + isNullIterator());
            System.out.println("min " + (Objects.isNull(min)? "null": min.toString()));
            System.out.println("max " + (Objects.isNull(max)? "null": max.toString()));
        }
        wrapped = tree.iterator(to.record, max, mapper);

        // We are voluntarily one step behind with the saved
        // `Record`. Calling `hasNext()` and `next()` recover
        // a clean internal state.
        hasNext();
        try {
            next();
        } catch (Exception e) {
            System.out.println("tree " + tree.getParams().toString());
            System.out.println("min " + tree.minKey().toString());
            System.out.println("max " + tree.maxKey().toString());
            System.out.println("to " + to.record.toString());
            System.out.println("is Singleton? " + isSingletonIterator());
            System.out.println("is Null ? " + isNullIterator());

            e.printStackTrace();
        }
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

}
