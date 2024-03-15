package fr.gdd.sage.sager.optimizers;

import fr.gdd.jena.visitors.ReturningOpVisitorRouter;
import org.apache.jena.sparql.algebra.Op;

/**
 * Create the plan that will be used by the Executor afterward.
 */
public class SagerOptimizer {

    public Op optimize(Op toOptimize) {
        toOptimize = ReturningOpVisitorRouter.visit(new BGP2Triples(), toOptimize);
        // TODO ordering, by sending estimated COUNT queries to the opened dataset
        // TODO This is already partially done in `SageOptimizer`
        return toOptimize;
    }

}
