package fr.gdd.server;

import org.apache.jena.atlas.lib.Lib;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.engine.Plan;
import org.apache.jena.sparql.engine.QueryEngineFactory;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.tdb2.TDBException;
import org.apache.jena.tdb2.solver.QueryEngineTDB;
import org.apache.jena.tdb2.store.DatasetGraphTDB;
import org.apache.jena.tdb2.sys.TDBInternal;
import org.apache.jena.sparql.algebra.Op ;
import org.apache.jena.sparql.core.DatasetGraph;



public class SageQueryEngine extends QueryEngineTDB {

    public SageQueryEngine (Query query, DatasetGraphTDB dataset, Binding input, Context cxt) {
        super(query, dataset, input, cxt);
    }

    public SageQueryEngine (Op op, DatasetGraphTDB dataset, Binding input, Context context) {
        super(op, dataset, input, context);
    }

    
    // ---- Factory
    protected static QueryEngineFactory factory = new SageQueryEngineFactory();

    protected static class SageQueryEngineFactory implements QueryEngineFactory {

        @Override
        public boolean accept(Query query, DatasetGraph dataset, Context context) {
            return TDBInternal.isBackedByTDB(dataset);
        }

        @Override
        public boolean accept(Op op, DatasetGraph dataset, Context context) {
            return TDBInternal.isBackedByTDB(dataset);
        }

        @Override
        public Plan create(Query query, DatasetGraph dataset, Binding binding, Context context) {
            SageQueryEngine engine = new SageQueryEngine(query, dsgToQuery(dataset), binding, context);
            return engine.getPlan();

        }

        @Override
        public Plan create(Op op, DatasetGraph dataset, Binding binding, Context context) {
            SageQueryEngine engine = new SageQueryEngine(op, dsgToQuery(dataset), binding, context);
            return engine.getPlan();
        }

        protected DatasetGraphTDB dsgToQuery(DatasetGraph dataset) {
            try {
                return TDBInternal.requireStorage(dataset);
            } catch (TDBException ex) {
                // Check to a more specific message.
                throw new TDBException("Internal inconsistency: trying to execute query on unrecognized kind of DatasetGraph: "+Lib.className(dataset));
            }
        }

    }
}
