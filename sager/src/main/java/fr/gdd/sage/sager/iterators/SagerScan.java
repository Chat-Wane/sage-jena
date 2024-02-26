package fr.gdd.sage.sager.iterators;

import fr.gdd.sage.interfaces.BackendIterator;
import fr.gdd.sage.interfaces.SPOC;
import fr.gdd.sage.sager.SagerConstants;
import fr.gdd.sage.sager.Save2SPARQL;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.atlas.lib.tuple.Tuple3;
import org.apache.jena.atlas.lib.tuple.TupleFactory;
import org.apache.jena.dboe.trans.bplustree.PreemptJenaIterator;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.tdb2.solver.BindingNodeId;
import org.apache.jena.tdb2.store.NodeId;

import java.util.Iterator;
import java.util.Objects;

public class SagerScan implements Iterator<BindingNodeId> {

    final Long deadline;
    final BackendIterator<NodeId, ?> wrapped;
    boolean first = true;
    final OpTriple op;

    Tuple3<Var> vars;

    public SagerScan(ExecutionContext context, OpTriple op, BackendIterator<NodeId, ?> wrapped) {
        this.deadline = context.getContext().getLong(SagerConstants.DEADLINE, Long.MAX_VALUE);
        this.wrapped = wrapped;
        this.op = op;
        Save2SPARQL saver = context.getContext().get(SagerConstants.SAVER);
        saver.register(op, wrapped);

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

        return result;
    }

    @Override
    public BindingNodeId next() {
        first = false; // at least one iteration
        wrapped.next();

        BindingNodeId binding = new BindingNodeId();
        if (Objects.nonNull(vars.get(0))) { // ugly x3
            binding.put(vars.get(0), wrapped.getId(SPOC.SUBJECT));
        }
        if (Objects.nonNull(vars.get(1))) {
            binding.put(vars.get(1), wrapped.getId(SPOC.PREDICATE));
        }
        if (Objects.nonNull(vars.get(2))) {
            binding.put(vars.get(2), wrapped.getId(SPOC.OBJECT));
        }
        return binding;
    }

    public SagerScan skip(Long offset) {
        // TODO for now, poor complexity, replace it with logarithmic skip
        long i = 0;
        while (i < offset) {
            wrapped.next();
            ++i;
        }
        return this;
    }

}
