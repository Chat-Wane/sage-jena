package fr.gdd.common;

import java.util.TreeMap;

import org.apache.jena.atlas.lib.Pair;



public class SageInput<SKIP> {

    private TreeMap<Integer, SKIP> state = new TreeMap<>();
    private int  limit   = Integer.MAX_VALUE;
    private int  timeout = Integer.MAX_VALUE;
    Backend backend;

    public SageInput() {
    }

    public SageInput(final int limit) {
        this.limit = limit;
    }
    
    public SageInput(final int limit, final int timeout) {
        this.limit = limit;
        this.timeout = timeout;
    }

    public void setBackend(Backend backend) {
        this.backend = backend;
    }

    public Backend getBackend() {
        return backend;
    }
    
    public void resume(Pair<Integer, SKIP>... states) {
        this.state = new TreeMap<>();
        for (Pair<Integer, SKIP> state : states) {
            this.state.put(state.getLeft(), state.getRight());
        }
    }

    public void setState(TreeMap<Integer, SKIP> state) {
        this.state = state;
    }

    public SKIP getState(Integer id) {
        if (state.containsKey(id)) {
            SKIP value = state.remove(id);
            return value;
        } else {
            return null;
        }
    }

    public TreeMap<Integer, SKIP> getState() {
        return state;
    }

    public int getLimit() {
	return limit;
    }

    public int getTimeout() {
        return timeout;
    }
    
}
