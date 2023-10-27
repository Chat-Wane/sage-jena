package org.apache.jena.sparql.engine.iterator;

import fr.gdd.sage.arq.IdentifierAllocator;
import fr.gdd.sage.arq.SageConstants;
import fr.gdd.sage.io.SageInput;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.Op2;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.engine.main.iterator.QueryIterUnion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Unions create an iterator that concatenate operation. We want this
 * iterator to remember the state it was on before pausing, so it can resume
 * and execute in the same state.
 **/
public class PreemptQueryIterUnion extends QueryIterUnion {

    private static Logger log = LoggerFactory.getLogger(PreemptQueryIterUnion.class);

    Integer id;
    SageInput sageInput;

    public PreemptQueryIterUnion(QueryIterator qIter, List<Op> subOps, ExecutionContext context) {
        super(qIter, subOps, context);
        sageInput = getExecContext().getContext().get(SageConstants.input);

        Integer current = getExecContext().getContext().get(SageConstants.cursor);
        IdentifierAllocator allocator = new IdentifierAllocator(current);
        this.id = allocator.getCurrent() + 1;

        log.debug("Union {} gets id {}", Arrays.toString(subOps.toArray()), id);


    }

    @Override
    protected QueryIterator nextStage(Binding binding) {
        getExecContext().getContext().set(SageConstants.cursor, id);

        PreemptQueryIterConcat unionQIter = new PreemptQueryIterConcat(getExecContext(), id);

        IdentifierAllocator idAlloc = new IdentifierAllocator(id);

        for (Op subOp : subOps) {
            subOp = QC.substitute(subOp, binding);
            QueryIterator parent = QueryIterSingleton.create(binding, getExecContext());
            QueryIterator qIter = QC.execute(subOp, parent, getExecContext());
            subOp.visit(idAlloc); // TODO bad complexity-wise to call it there every time
            getExecContext().getContext().set(SageConstants.cursor, idAlloc.getCurrent());
            unionQIter.add(qIter) ;
        }

        if (Objects.nonNull(sageInput.getState()) && sageInput.getState().containsKey(id)) {
            // TODO an optimization consists in not QC.execute(subOp) for skipped subOp
            unionQIter.skip((int) sageInput.getState(id));
        }

        return unionQIter ;
    }
}
