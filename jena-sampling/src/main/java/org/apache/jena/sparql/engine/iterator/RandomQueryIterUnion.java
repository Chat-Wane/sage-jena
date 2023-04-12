package org.apache.jena.sparql.engine.iterator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import com.github.jsonldjava.utils.Obj;
import fr.gdd.sage.arq.SageConstants;
import fr.gdd.sage.io.SageInput;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.QueryIterSingleton;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.engine.main.iterator.QueryIterUnion;
import org.apache.jena.util.iterator.ClosableIterator;

/**
 * Unions create an iterator that concatenate operation. We want this
 * iterator to be randomized, therefore, each `nextStage` randomizes
 * the list of operations. 
 **/
public class RandomQueryIterUnion extends QueryIter1 {

    List<Op> initialOps;

    QueryIterator current;

    SageInput<?> input;

    boolean isFirstExecution = true;
    Binding initialBinding;

    public RandomQueryIterUnion(QueryIterator input,
                                List<Op> subOps,
                                ExecutionContext context) {
        super(input, context);
        initialOps = new ArrayList<>(subOps);
        this.input = context.getContext().get(SageConstants.input);
        initialBinding = getInput().next();
    }

    @Override
    protected boolean hasNextBinding() {
        if (Objects.nonNull(current)) {
            performClose(current);
        }
        if (!isFirstExecution || System.currentTimeMillis() >= input.getDeadline()) {
            performClose(getInput());
            return false;
        }
        return true;
    }

    @Override
    protected Binding moveToNextBinding() {
        if (Objects.nonNull(current)) {
            performClose(current);
        }

        Collections.shuffle(initialOps);
        Op subOp = QC.substitute(initialOps.get(0), initialBinding) ;
        QueryIterator parent = QueryIterSingleton.create(initialBinding, getExecContext()) ;
        current = QC.execute(subOp, parent, getExecContext()) ;
        return current.next();
    }

    @Override
    protected void requestSubCancel() {
    }

    @Override
    protected void closeSubIterator() {
    }
}
