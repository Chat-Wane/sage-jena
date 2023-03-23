package org.apache.jena.sparql.engine.iterator;

import fr.gdd.sage.arq.SageConstants;
import fr.gdd.sage.generics.Pair;
import fr.gdd.sage.io.SageInput;
import fr.gdd.sage.io.SageOutput;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.engine.main.iterator.QueryIterUnion;
import org.apache.jena.util.iterator.NullIterator;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Unions create an iterator that concatenate operation. We want this
 * iterator to remember the state it was on before pausing so it can resume
 * and execute in the same state. (TODO) update text
 **/
public class PreemptQueryIterUnion extends QueryIterUnion {

    public PreemptQueryIterUnion(QueryIterator input,
                                List<Op> subOps,
                                ExecutionContext context) {
        super(input, subOps, context);
    }

    @Override
    protected QueryIterator nextStage(Binding binding) {
        PreemptQueryIterConcat unionQIter = new PreemptQueryIterConcat(getExecContext());
        for (Op subOp : subOps) {
            subOp = QC.substitute(subOp, binding) ;
            QueryIterator parent = QueryIterSingleton.create(binding, getExecContext()) ;
            QueryIterator qIter = QC.execute(subOp, parent, getExecContext()) ;
            unionQIter.add(qIter) ;
        }

        return unionQIter;
    }
}
