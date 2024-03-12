package fr.gdd.sage.sager.iterators;

import fr.gdd.jena.visitors.ReturningArgsOpVisitorRouter;
import fr.gdd.jena.visitors.ReturningOpVisitorRouter;
import fr.gdd.sage.sager.BindingId2Value;
import fr.gdd.sage.sager.SagerOpExecutor;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class SagerUnion implements Iterator<BindingId2Value> {

    final Iterator<BindingId2Value> input;
    final Op left;
    final Op right;
    final SagerOpExecutor executor;

    BindingId2Value current = null;
    Integer currentOp = -1; // -1 not init, 0 left, 1 right
    Iterator<BindingId2Value> currentIt;

    public SagerUnion(SagerOpExecutor executor, Iterator<BindingId2Value> input, Op left, Op right) {
        this.left = left;
        this.right = right;
        this.input = input;
        this.executor = executor;
    }

    @Override
    public boolean hasNext() {

        while (input.hasNext() && !currentIt.hasNext()) {
            current = input.next();
            if (currentOp < 0) {
                currentOp = 0;
                currentIt = ReturningArgsOpVisitorRouter.visit(executor, left, Iter.of(current));
            }

            if (currentOp == 0 && !currentIt.hasNext()) {
                currentOp = 1;
                currentIt = ReturningArgsOpVisitorRouter.visit(executor, right, Iter.of(current));
            }

            if (currentOp == 1 && !currentIt.hasNext()) {
                currentOp = -1;
            }
        }

        return currentIt.hasNext();
    }

    @Override
    public BindingId2Value next() {
        return currentIt.next();
    }
}
