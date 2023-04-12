package org.apache.jena.sparql.engine.iterator;

import fr.gdd.sage.arq.SageConstants;
import fr.gdd.sage.io.SageInput;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.main.QC;

import java.util.Objects;

/**
 * Similarly to random unions, random nested loop join operators takes
 * a random binding as input from its left hand side. Then tries to join
 * with a random binding from its right hand side.
 */
public class RandQueryIterNestedLoopJoin extends QueryIter1 {

    boolean isFirstExecution = true;

    Binding left;
    QueryIterator leftIterator;
    QueryIterator rightIterator;
    OpJoin op;

    SageInput<?> input;

    public RandQueryIterNestedLoopJoin(OpJoin opJoin, QueryIterator input, ExecutionContext context) {
        super(input, context);
        leftIterator = QC.execute(opJoin.getLeft(), QueryIterRoot.create(getExecContext()), context);
        left = leftIterator.hasNext() ? leftIterator.next(): null;
        this.op = opJoin;
        this.input = context.getContext().get(SageConstants.input);
    }

    @Override
    protected boolean hasNextBinding() {
        if (!isFirstExecution || System.currentTimeMillis() >= input.getDeadline() || Objects.isNull(left)) {
            return false;
        }
        isFirstExecution = false;

        rightIterator = QC.execute(op.getRight(), getInput(), getExecContext());
        return rightIterator.hasNext();
    }

    @Override
    protected Binding moveToNextBinding() {
        return Algebra.merge(left, rightIterator.next());
    }


    @Override
    protected void requestSubCancel() {
        requestSubCancel() ;
        performRequestCancel(leftIterator);
        performRequestCancel(rightIterator);
    }

    @Override
    protected void closeSubIterator() {
        closeSubIterator();
        performClose(leftIterator);
        performClose(rightIterator);
    }
}
