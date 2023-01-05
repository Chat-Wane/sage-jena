package fr.gdd.common;

import java.util.ArrayList;
import java.util.TreeMap;

import org.apache.jena.atlas.lib.Pair;



public class SageResult<SKIP> {

    private ArrayList<ArrayList<String>> results = new ArrayList<>();
    private TreeMap<Integer, SKIP> state = new TreeMap<>();

    public SageResult() { }

    public void addResult(ArrayList<String> result) {
        results.add(result);
    }

    public void save(Pair<Integer, SKIP>... states) {
        for (Pair<Integer, SKIP> state : states) {
            this.state.put(state.getLeft(), state.getRight());
        }
    }
    
}
