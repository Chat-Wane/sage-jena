package fr.gdd.sage.sager.iterators;

import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.QueryIteratorWrapper;

public class SagerRoot extends QueryIteratorWrapper {

    boolean doesHaveNext = false;
    boolean consumed = true;
    Binding buffered = null;

    public SagerRoot(QueryIterator qIter) {
        super(qIter);
    }

    @Override
    public boolean hasNextBinding() {
        if (!consumed) {
            return doesHaveNext;
        }

        try {
            doesHaveNext = super.hasNextBinding();
        } catch (PauseException e) {
            close();
            return false;
        }


        if (doesHaveNext) {
            try {
                buffered = super.moveToNextBinding();
                // may save during the `.next()` which would set `.hasNext()` as false while
                // it expects and checks `true`. When it happens, it throws a `NoSuchElementException`
            } catch (PauseException e) {
                close();
                return false;
            }
            consumed = false;
            return true;
        }
        return false;
    }

    @Override
    protected Binding moveToNextBinding() {
        consumed = true;
        return buffered;
    }
}