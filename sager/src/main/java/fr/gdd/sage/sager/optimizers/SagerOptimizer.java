package fr.gdd.sage.sager.optimizers;

import fr.gdd.jena.visitors.ReturningOpVisitorRouter;
import org.apache.jena.sparql.algebra.Op;

/**
 * Create the plan that will be used by the Executor afterward.
 */
public class SagerOptimizer {

    Offset2Skip offset2skip = new Offset2Skip();

    public Op optimize(Op toOptimize) {
        // TODO ordering, by sending estimated COUNT queries to the opened dataset
        // TODO This is already partially done in `SageOptimizer`
        return ReturningOpVisitorRouter.visit(offset2skip, toOptimize);
    }

    public Offset2Skip getOffset2skip() {
        return offset2skip;
    }
}
