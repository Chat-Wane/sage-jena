package fr.gdd.common;

import java.util.ArrayList;
import java.util.TreeMap;

import org.apache.jena.atlas.lib.Pair;



public class SageOutput<SKIP> {

    private ArrayList<ArrayList<String>> results = new ArrayList<>();
    private TreeMap<Integer, SKIP> state = null;

    public SageOutput() { }

    public void addResult(ArrayList<String> result) {
        results.add(result);
    }

    public void save(Pair<Integer, SKIP>... states) {
        this.state = new TreeMap<>();
        for (Pair<Integer, SKIP> s : states) {
            this.state.put(s.getLeft(), s.getRight());
        }
    }

    public void addState(Pair<Integer, SKIP> state) {
        if (this.state == null) {
            this.state = new TreeMap<>();
        }
        this.state.put(state.getLeft(), state.getRight());
    }

    public void merge(SageOutput<SKIP> other) {
        this.results.addAll(other.getResults());
        this.state = other.state;
    }
    
    public ArrayList<ArrayList<String>> getResults() {
        return results;
    }

    public int size() {
        return results.size();
    }

    public TreeMap<Integer, SKIP> getState() {
        return state;
    }
    
}
