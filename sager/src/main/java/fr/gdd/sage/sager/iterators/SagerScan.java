package fr.gdd.sage.sager.iterators;

import fr.gdd.sage.interfaces.BackendIterator;
import fr.gdd.sage.sager.SagerConstants;
import fr.gdd.sage.sager.Save2SPARQL;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.dboe.trans.bplustree.PreemptJenaIterator;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.tdb2.store.NodeId;

import java.util.Iterator;

public class SagerScan implements Iterator<Tuple<NodeId>> {

    final Long deadline;
    final BackendIterator<?, ?> wrapped;
    boolean first = true;
    final Op op;

    public SagerScan(ExecutionContext context, Op op, BackendIterator<?, ?> wrapped) {
        this.deadline = context.getContext().getLong(SagerConstants.DEADLINE, Long.MAX_VALUE);
        this.wrapped = wrapped;
        this.op = op;
        Save2SPARQL saver = context.getContext().get(SagerConstants.SAVER);
        saver.register(op, wrapped);
    }

    @Override
    public boolean hasNext() {
        boolean result = wrapped.hasNext();

        if (result && !first && System.currentTimeMillis() >= deadline) {
            // execution stops immediately, caught by {@link PreemptRootIter}
            throw new PauseException(op);
        }

        return result;
    }

    @Override
    public Tuple<NodeId> next() {
        first = false;

        wrapped.next();
        return ((PreemptJenaIterator) wrapped).getCurrentTuple(); // TODO no cast
    }

}
