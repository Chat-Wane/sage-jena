package fr.gdd.sage;

import fr.gdd.sage.arq.QueryEngineSage;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpLib;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.engine.*;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.PreemptCounterIter;
import org.apache.jena.sparql.engine.iterator.QueryIterRoot;
import org.apache.jena.sparql.engine.iterator.QueryIteratorCheck;
import org.apache.jena.sparql.engine.iterator.QueryIteratorTiming;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.mgt.Explain;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.tdb2.TDB2;
import org.apache.jena.tdb2.solver.QueryEngineTDB;
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
        QC.setFactory(dataset.getContext(), new OpExecutorRandom.OpExecutorRandomFactory(context));
    }

    protected QueryEngineRandom(Query query, DatasetGraphTDB dataset, Binding input, Context context) {
        super(query, dataset, input, context);
        QC.setFactory(dataset.getContext(), new OpExecutorRandom.OpExecutorRandomFactory(context));
    }

    static public void register() {
        QueryEngineRegistry.addFactory(factory);
    }

    static public void unregister() {
        QueryEngineRegistry.removeFactory(factory);
    }


    private static boolean isUnionDefaultGraph(Context cxt) {
        return cxt.isTrue(TDB2.symUnionDefaultGraph1) || cxt.isTrue(TDB2.symUnionDefaultGraph2);
    }

    @Override
    public QueryIterator eval(Op op, DatasetGraph dsg, Binding input, Context context) {
        // #1 an explain that comes from {@link QueryEngineTDB}
        if ( isUnionDefaultGraph(context) && !isDynamicDataset() ) {
            op = OpLib.unionDefaultGraphQuads(op) ;
            Explain.explain("REWRITE(Union default graph)", op, context);
        }

        // #2 comes from {@link QueryEngineBase}
        ExecutionContext execCxt = new ExecutionContext(context, dsg.getDefaultGraph(), dsg, QC.getFactory(context)) ;
        QueryIterator qIter1 =
                ( input.isEmpty() ) ? QueryIterRoot.create(execCxt)
                        : QueryIterRoot.create(input, execCxt);
        QueryIterator qIter = QC.execute(op, qIter1, execCxt) ;

        // #3 inbetween we add our home-made counter iterator :)
        // (TODO) (TODO) (TODO)
        PreemptCounterIter counterIter = new PreemptCounterIter(qIter, execCxt);

        // Wrap with something to check for closed iterators.
        qIter = QueryIteratorCheck.check(counterIter, execCxt) ;
        // Need call back.
        if ( context.isTrue(ARQ.enableExecutionTimeLogging) )
            qIter = QueryIteratorTiming.time(qIter) ;
        return qIter ;
    }

    /* ******************** Factory ********************** */
    public static QueryEngineFactory factory = new QueryEngineRandomFactory();

    /**
     * Mostly identical to {@link org.apache.jena.tdb2.solver.QueryEngineTDB.QueryEngineFactoryTDB}
     * but calling {@link QueryEngineRandom} instead of {@link QueryEngineTDB} to build plans.
     */
    public static class QueryEngineRandomFactory extends QueryEngineFactoryTDB {

        @Override
        public Plan create(Query query, DatasetGraph dataset, Binding input, Context context) {
            QueryEngineSage engine = new QueryEngineRandom(query, dsgToQuery(dataset), input, context);
            return engine.getPlan();
        }

        @Override
        public Plan create(Op op, DatasetGraph dataset, Binding binding, Context context) {
            QueryEngineSage engine = new QueryEngineRandom(op, dsgToQuery(dataset), binding, context);
            return engine.getPlan();
        }
    }
}
