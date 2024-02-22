package fr.gdd.sage.sager.iterators;

import fr.gdd.sage.sager.SagerConstants;
import fr.gdd.sage.sager.Save2SPARQL;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.tdb2.solver.BindingNodeId;

import java.util.Iterator;

public class SagerRoot implements Iterator<BindingNodeId> {

    final Save2SPARQL saver;
    final Iterator<BindingNodeId> wrapped;

    boolean doesHaveNext = false;
    boolean consumed = true;
    BindingNodeId buffered = null;

    public SagerRoot(ExecutionContext context, Iterator<BindingNodeId> wrapped) {
        this.wrapped = wrapped;
        this.saver = context.getContext().get(SagerConstants.SAVER);
    }

    @Override
    public boolean hasNext() {
        if (!consumed) {
            return doesHaveNext;
        }

        try {
            doesHaveNext = wrapped.hasNext();
        } catch (PauseException e) {
            // close(); TODO
            this.saver.save(e.caller);
            return false;
        }

        if (doesHaveNext) {
            try {
                buffered = wrapped.next();
                // may save during the `.next()` which would set `.hasNext()` as false while
                // it expects and checks `true`. When it happens, it throws a `NoSuchElementException`
            } catch (PauseException e) {
                // close(); TODO
                this.saver.save(e.caller);
                return false;
            }
            consumed = false;
            return true;
        }
        return false;
    }

    @Override
    public BindingNodeId next() {
        consumed = true;
        return buffered;
    }
}