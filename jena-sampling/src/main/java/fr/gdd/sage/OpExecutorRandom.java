package fr.gdd.sage;

import fr.gdd.sage.arq.OpExecutorSage;
import fr.gdd.sage.configuration.SageServerConfiguration;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpUnion;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;

/**
 * (TODO) Overload every `execute` function of interest. Bgps, triple, quads ones are modified using the scan factory.
 */
public class OpExecutorRandom extends OpExecutorSage {

    // (TODO) factory

    public OpExecutorRandom(ExecutionContext context, SageServerConfiguration configuration) {
        super(context, configuration);
    }

    @Override
    public QueryIterator execute(OpUnion union, QueryIterator input) {
        // (TODO)
        return super.execute(union, input);
    }

    @Override
    protected QueryIterator execute(OpJoin opJoin, QueryIterator input) {
        // (TODO)
        return super.execute(opJoin, input);
    }

}
