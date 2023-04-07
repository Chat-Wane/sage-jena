package fr.gdd.sage;

import fr.gdd.sage.arq.QueryEngineSage;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.tdb2.store.DatasetGraphTDB;

/**
 * (TODO) instead of a {@link org.apache.jena.sparql.engine.iterator.PreemptCounterIter}, we have
 * never ending random walks. Can also count to get the number of root retries.
 *
 * (TODO) also register the scan factory here, that will be used in the {@link org.apache.jena.tdb2.solver.PatternMatchSage}
 * and {@link org.apache.jena.tdb2.solver.PreemptStageMatchTuple}
 *
 * (TODO) register {@link OpExecutorRandom}
 */
public class QueryEngineRandom extends QueryEngineSage {

    protected QueryEngineRandom(Op op, DatasetGraphTDB dataset, Binding input, Context context) {
        super(op, dataset, input, context);
    }

    protected QueryEngineRandom(Query query, DatasetGraphTDB dataset, Binding input, Context cxt) {
        super(query, dataset, input, cxt);
    }
}
