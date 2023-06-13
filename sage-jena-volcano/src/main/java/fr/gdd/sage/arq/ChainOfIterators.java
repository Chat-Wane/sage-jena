package fr.gdd.sage.arq;

import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

/**
 * To save its state, iterators needs to know where they are at in the chain of execution
 * to (efficiently) ask for their parents' state.
 */
public class ChainOfIterators {

    HashMap<Integer, Integer> childToParent = new HashMap<>();

    /**
     * Initialize an empty chain of iterator in the execution context, if it does not exist. Keep
     * the old one otherwise.
     * @param ec The execution context of the query.
     */
    public static void create(ExecutionContext ec) {
        ec.getContext().setIfUndef(SageConstants.chain, new ChainOfIterators());
    }

    /**
     * State that the iterator child is the child of parent.
     * @param parent The identifier of the parent iterator.
     * @param child The child iterator.
     */
    public void add(Integer parent, Integer child) {
        childToParent.put(child, parent);
    }

    /**
     * @param child The unique identifier of the iterator
     * @return A list of parents of the iterator.
     */
    public List<Integer> getParents(Integer child) {
        boolean done = false;
        List<Integer> parents = new ArrayList<>();
        while (!done) {
            Integer parent = childToParent.get(child);
            if (Objects.nonNull(parent)) {
                parents.add(parent);
                child = parent;
            } else {
                done = true;
            }
        }
        return parents;
    }

}
