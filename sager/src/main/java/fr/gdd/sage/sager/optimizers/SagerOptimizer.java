package fr.gdd.sage.sager.optimizers;

import org.apache.jena.sparql.algebra.Op;

/**
 * Create the plan that will be used by the Executor afterward.
 */
public class SagerOptimizer {

    public static Op optimize(Op toOptimize) {
        // TODO ordering, by sending estimated COUNT queries to the opened dataset
        // TODO This is already partially done in `SageOptimizer`
        return null;
    }

}
