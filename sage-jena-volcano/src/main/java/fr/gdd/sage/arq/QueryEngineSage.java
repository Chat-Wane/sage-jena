package fr.gdd.sage.arq;

import fr.gdd.sage.generics.Pair;
import org.apache.jena.atlas.lib.Lib;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpLib;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.Plan;
import org.apache.jena.sparql.engine.QueryEngineFactory;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.*;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.mgt.Explain;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.tdb2.TDB2;
import org.apache.jena.tdb2.TDBException;
import org.apache.jena.tdb2.solver.QueryEngineTDB;
import org.apache.jena.tdb2.store.DatasetGraphTDB;
import org.apache.jena.tdb2.sys.TDBInternal;

/**
 * Instead of relying on {@link org.apache.jena.tdb2.solver.QueryEngineTDB} to
 * call our {@link OpExecutorSage}, we create and register our engine. We add a necessary
 * counter in top of the execution pipeline.
 */
public class QueryEngineSage extends QueryEngineTDB {

    protected QueryEngineSage(Op op, DatasetGraphTDB dataset, Binding input, Context context) {
        super(op, dataset, input, context);
    }

    protected QueryEngineSage(Query query, DatasetGraphTDB dataset, Binding input, Context cxt) {
        super(query, dataset, input, cxt);
    }

    private static boolean isUnionDefaultGraph(Context cxt) {
        return cxt.isTrue(TDB2.symUnionDefaultGraph1) || cxt.isTrue(TDB2.symUnionDefaultGraph2);
    }

    @Override
    public QueryIterator eval(Op op, DatasetGraph dsg, Binding input, Context context) {
        // #1 an explain that comes from {@link QueryEngineTDB}
        if ( isUnionDefaultGraph(context) && ! isDynamicDataset() ) {
            op = OpLib.unionDefaultGraphQuads(op) ;
            Explain.explain("REWRITE(Union default graph)", op, context);
        }

        // #2 comes from {@link QueryEngineBase}
        ExecutionContext execCxt = new ExecutionContext(context, dsg.getDefaultGraph(), dsg, QC.getFactory(context)) ;
        QueryIterator qIter1 =
                ( input.isEmpty() ) ? QueryIterRoot.create(execCxt)
                        : QueryIterRoot.create(input, execCxt);
        QueryIterator qIter = QC.execute(op, qIter1, execCxt) ;

        // #3 inbetween we add our home-made slice iterator :)
        // PreemptQueryIterSlice meow = new PreemptQueryIterSlice(qIter, 0, 5, execCxt);
        PreemptCounterIter counterIter = new PreemptCounterIter(qIter, execCxt);

        // Wrap with something to check for closed iterators.
        qIter = QueryIteratorCheck.check(counterIter, execCxt) ;
        // Need call back.
        if ( context.isTrue(ARQ.enableExecutionTimeLogging) )
            qIter = QueryIteratorTiming.time(qIter) ;
        return qIter ;
    }

    // ---- Factory
    public static QueryEngineFactory factory = new QueryEngineSage.QueryEngineFactorySage();

    /**
     * True copy pasta of {@link org.apache.jena.tdb2.solver.QueryEngineTDB.QueryEngineFactoryTDB}
     * replacing
     */
    protected static class QueryEngineFactorySage extends QueryEngineFactoryTDB {

        protected DatasetGraphTDB dsgToQuery(DatasetGraph dataset) {
            try {
                return TDBInternal.requireStorage(dataset);
            } catch (TDBException ex) {
                // Check to a more specific message.
                throw new TDBException("Internal inconsistency: trying to execute query on unrecognized kind of DatasetGraph: "+ Lib.className(dataset));
            }
        }

        @Override
        public boolean accept(Query query, DatasetGraph dataset, Context context) {
            return TDBInternal.isBackedByTDB(dataset);
        }

        @Override
        public Plan create(Query query, DatasetGraph dataset, Binding input, Context context) {
            QueryEngineSage engine = new QueryEngineSage(query, dsgToQuery(dataset), input, context);
            return engine.getPlan();
        }

        @Override
        public boolean accept(Op op, DatasetGraph dataset, Context context) {
            return TDBInternal.isBackedByTDB(dataset);
        }

        @Override
        public Plan create(Op op, DatasetGraph dataset, Binding binding, Context context) {
            QueryEngineSage engine = new QueryEngineSage(op, dsgToQuery(dataset), binding, context);
            return engine.getPlan();
        }
    }
}
