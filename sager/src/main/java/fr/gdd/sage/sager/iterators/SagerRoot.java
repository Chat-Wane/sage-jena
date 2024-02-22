package fr.gdd.sage.sager.iterators;

import org.apache.jena.tdb2.solver.BindingNodeId;

import java.util.Iterator;

public class SagerRoot implements Iterator<BindingNodeId> {

    Iterator<BindingNodeId> wrapped;
    boolean doesHaveNext = false;
    boolean consumed = true;
    BindingNodeId buffered = null;

    public SagerRoot(Iterator<BindingNodeId> wrapped) {
        this.wrapped = wrapped;
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
            return false;
        }

        if (doesHaveNext) {
            try {
                buffered = wrapped.next();
                // may save during the `.next()` which would set `.hasNext()` as false while
                // it expects and checks `true`. When it happens, it throws a `NoSuchElementException`
            } catch (PauseException e) {
                // close(); TODO
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