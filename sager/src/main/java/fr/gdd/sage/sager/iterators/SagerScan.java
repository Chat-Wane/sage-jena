package fr.gdd.sage.sager.iterators;

import fr.gdd.sage.generics.LazyIterator;
import fr.gdd.sage.interfaces.BackendIterator;
import fr.gdd.sage.interfaces.SPOC;
import fr.gdd.sage.jena.JenaBackend;
import fr.gdd.sage.sager.BindingId2Value;
import fr.gdd.sage.sager.SagerConstants;
import fr.gdd.sage.sager.pause.Save2SPARQL;
import org.apache.jena.atlas.lib.tuple.Tuple3;
import org.apache.jena.atlas.lib.tuple.TupleFactory;
import org.apache.jena.dboe.trans.bplustree.ProgressJenaIterator;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.tdb2.store.NodeId;

import java.util.Iterator;
import java.util.Objects;

public class SagerScan implements Iterator<BindingId2Value> {

    final Long deadline;
    final BackendIterator<NodeId, ?> wrapped;
    boolean first = true;
    final OpTriple op;
    BindingId2Value current;
    final Save2SPARQL saver;
    JenaBackend backend;

    Tuple3<Var> vars;

    public SagerScan(ExecutionContext context, OpTriple op, BackendIterator<NodeId, ?> wrapped) {
        this.deadline = context.getContext().getLong(SagerConstants.DEADLINE, Long.MAX_VALUE);
        this.backend = context.getContext().get(SagerConstants.BACKEND);
        this.wrapped = wrapped;
        this.op = op;
        this.saver = context.getContext().get(SagerConstants.SAVER);
        saver.register(op, this);

        vars = TupleFactory.create3(
                op.getTriple().getSubject().isVariable() ? Var.alloc(op.getTriple().getSubject()) : null,
                op.getTriple().getPredicate().isVariable() ? Var.alloc(op.getTriple().getPredicate()) : null,
                op.getTriple().getObject().isVariable() ? Var.alloc(op.getTriple().getObject()) : null);
    }

    @Override
    public boolean hasNext() {
        boolean result = wrapped.hasNext();

        if (result && !first && System.currentTimeMillis() >= deadline) {
            // execution stops immediately, caught by {@link PreemptRootIter}
            throw new PauseException(op);
        }

        if (!result) { saver.unregister(op); }

        return result;
    }

    @Override
    public BindingId2Value next() {
        first = false; // at least one iteration
        wrapped.next();

        current = new BindingId2Value().setDefaultTable(backend.getNodeTripleTable()); // TODO quads

        if (Objects.nonNull(vars.get(0))) { // ugly x3
            current.put(vars.get(0), wrapped.getId(SPOC.SUBJECT));
        }
        if (Objects.nonNull(vars.get(1))) {
            current.put(vars.get(1), wrapped.getId(SPOC.PREDICATE));
        }
        if (Objects.nonNull(vars.get(2))) {
            current.put(vars.get(2), wrapped.getId(SPOC.OBJECT));
        }

        return current;
    }

    public BindingId2Value current() {
        return this.current;
    }

    public SagerScan skip(Long offset) {
        // TODO for now, poor complexity, replace it with logarithmic skip
        long i = 0;
        while (i < offset) {
            wrapped.hasNext();
            wrapped.next();
            ++i;
        }
        return this;
    }

    public Long offset() {
        // TODO remove casts
        return ((ProgressJenaIterator)((LazyIterator) this.wrapped).iterator).getOffset();
    }

}
