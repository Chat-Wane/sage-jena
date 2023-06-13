package fr.gdd.sage.arq;

import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sparql.engine.binding.Binding;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

public class ResultSetSage implements ResultSet {

    ResultSet wrapped;
    QuerySolution buffered;
    Map<Integer, ?> state;
    int rowNumber = 0;

    public ResultSetSage(ResultSet wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public boolean hasNext() {
        boolean doesHaveNext;
        try {
            doesHaveNext = wrapped.hasNext();
        } catch (PauseException e) {
            close();
            doesHaveNext = false;
        }


        if (doesHaveNext) {
            try {
                buffered = wrapped.next();
                // may save during the `.next()` which would set `.hasNext()` as false while
                // it expects and checks `true`. When it happens, it throws a `NoSuchElementException`
            } catch (PauseException e) {
                close();
                return false;
            }
            rowNumber += 1;
            return true;
        }
        return false;
    }

    @Override
    public QuerySolution next() {
        return buffered;
    }

    @Override
    public QuerySolution nextSolution() {
        return buffered;
    }

    @Override
    public Binding nextBinding() {
        return wrapped.nextBinding();
    }

    @Override
    public int getRowNumber() {
        return rowNumber;
    }

    @Override
    public List<String> getResultVars() {
        return wrapped.getResultVars();
    }

    @Override
    public Model getResourceModel() {
        return wrapped.getResourceModel();
    }

    @Override
    public void close() {
        wrapped.close();
    }
}
