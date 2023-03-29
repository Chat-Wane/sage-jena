package org.apache.jena.sparql.engine.iterator;

import fr.gdd.sage.arq.SageConstants;
import fr.gdd.sage.io.SageInput;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.engine.main.iterator.QueryIterUnion;

import java.util.List;
import java.util.Objects;

/**
 * Unions create an iterator that concatenate operation. We want this
 * iterator to remember the state it was on before pausing so it can resume
 * and execute in the same state. (TODO) update text
 **/
public class PreemptQueryIterUnion extends QueryIterUnion {

    SageInput<?> input;

    public PreemptQueryIterUnion(QueryIterator qIter, List<Op> subOps, ExecutionContext context) {
        super(qIter, subOps, context);
        input = getExecContext().getContext().get(SageConstants.input);
    }

    @Override
    protected QueryIterator nextStage(Binding binding) {
        Integer id = getExecContext().getContext().get(SageConstants.cursor);
        id += 1;
        getExecContext().getContext().set(SageConstants.cursor, id);

        PreemptQueryIterConcat unionQIter = new PreemptQueryIterConcat(getExecContext(), id);
        for (Op subOp : subOps) {
            subOp = QC.substitute(subOp, binding);
            QueryIterator parent = QueryIterSingleton.create(binding, getExecContext());
            QueryIterator qIter = QC.execute(subOp, parent, getExecContext());
            unionQIter.add(qIter);
        }

        if (Objects.nonNull(input.getState()) && input.getState().containsKey(id)) {
            unionQIter.skip((int) input.getState(id));
        }

        return unionQIter;
    }
}
