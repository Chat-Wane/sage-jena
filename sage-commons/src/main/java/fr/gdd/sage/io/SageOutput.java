package fr.gdd.sage.io;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;

import fr.gdd.sage.generics.Pair;



/**
 * All the data returned by a Sage query execution. Notably, it
 * returns a `state` containing the necessary data to resume the query
 * execution if need be.
 **/
public class SageOutput<SKIP extends Serializable> implements Serializable {

    private List<String> projections   = new ArrayList<>();
    private List<List<String>> results = new ArrayList<>();
    
    private TreeMap<Integer, SKIP> state = null;


    
    public SageOutput() {}
    
    public SageOutput(List<String> projections) {
        this.projections = projections;
    }

    public void add(List<String> result) {
        results.add(result);
    }

    public void addState(Pair<Integer, SKIP> state) {
        if (this.state == null) {
            this.state = new TreeMap<>();
        }
        this.state.put(state.left, state.right);
    }

    @SafeVarargs
    public final void save(Pair<Integer, SKIP>... states) {
        this.state = new TreeMap<>();
        for (Pair<Integer, SKIP> s : states) {
            this.state.put(s.left, s.right);
        }
    }

    public void merge(SageOutput<SKIP> other) {
        this.results.addAll(other.getResults());
        this.state = other.state;
    }
    
    public List<List<String>> getResults() {
        return results;
    }

    public int size() {
        return results.size();
    }

    public TreeMap<Integer, SKIP> getState() {
        //        if (Objects.nonNull(state)) {
        //            state.values().removeIf(Objects::isNull);
        // }
        return state;
    }

    public List<String> getProjections() {
        return projections;
    }
}
