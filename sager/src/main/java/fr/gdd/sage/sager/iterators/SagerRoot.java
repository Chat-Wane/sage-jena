package fr.gdd.sage.sager.iterators;

import fr.gdd.sage.sager.SagerConstants;
import fr.gdd.sage.sager.Save2SPARQL;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.Iterator;

/**
 * Iterator that is in charge of catching the Pause event thrown
 * by a downstream iterator.
 * @param <T> The type returned by the iterators.
 */
public class SagerRoot<T> implements Iterator<T> {

    final Save2SPARQL saver;
    final Iterator<T> wrapped;

    boolean doesHaveNext = false;
    boolean consumed = true;
    T buffered = null;

    public SagerRoot(ExecutionContext context, Iterator<T> wrapped) {
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
    public T next() {
        consumed = true;
        return buffered;
    }
}