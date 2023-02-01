package fr.gdd.server;

import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.fuseki.servlets.HttpAction;
import org.apache.jena.fuseki.servlets.SPARQL_QueryDataset;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.engine.QueryEngineFactory;
import org.apache.jena.sparql.engine.QueryEngineFactoryWrapper;
import org.apache.jena.sparql.engine.QueryEngineRegistry;
import org.apache.jena.sparql.engine.main.StageGenerator;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.sparql.util.ContextAccumulator;

import fr.gdd.volcano.SageOpExecutorFactory;
import fr.gdd.volcano.SageStageGenerator;

public class Sage_QueryDataset extends SPARQL_QueryDataset {

    public Sage_QueryDataset() {}

    @Override
    public void execAny(String methodName, HttpAction action) {
        System.out.printf("ANY %s \n", methodName);
        super.execAny(methodName, action);
    }

    
    @Override
    protected void execute(String queryString, HttpAction action) {
        var req = action.getRequest();
        System.out.printf("REQ  : %s \n", req.getHeaderNames());

        Query query = QueryFactory.create(queryString);

        System.out.printf("QUERY %s \n", query.toString());
        
        Pair<DatasetGraph, Query> p = decideDataset(action, query, query.toString());
        DatasetGraph dataset = p.getLeft();
        Query q = p.getRight();

        // System.out.printf("DATASET CHOSEN %s \n", dataset.getClass().getName());

        

        
        ContextAccumulator contextAcc =
            ContextAccumulator.newBuilder(()->ARQ.getContext(), ()->Context.fromDataset(dataset));
        QueryEngineFactory qef = QueryEngineRegistry.findFactory(query, dataset, contextAcc.context());
        System.out.printf("FACTORY CHOSEN : %s\n", qef.getClass().getName());

        Context c = contextAcc.context();
        for (var key : c.keys()) {
            System.out.printf("CONTEXT %s : %s\n", key, c.get(key));
        }
        
        
        super.execute(queryString, action);
    }

    @Override
    protected QueryExecution createQueryExecution(HttpAction action, Query query, DatasetGraph dataset) {
        System.out.println("SAGE QUERY EXECUTION CREATE");
        System.out.printf("DATASET : %s\n", dataset.toString());

        SageOpExecutorFactory sageFactory = new SageOpExecutorFactory();
        // QueryEngineRegistry.addFactory(sageFactory);
        
        QueryEngineFactory qeFactory = QueryEngineRegistry.findFactory(query, dataset, ARQ.getContext());
        System.out.printf("THEN FACTORY CHOSEN : %s\n", qeFactory.getClass().getName());
                
        var qe = QueryExecutionFactory.create(query, dataset);
        QC.setFactory(qe.getContext(), sageFactory);

        // for (var key : qe.getContext().keys()) {
        //     System.out.printf("QE CONTEXT %s : %s\n", key, qe.getContext().get(key));
        // }

        // ResultSet result_set = qe.execSelect();
        
        // while (result_set.hasNext() ) {
        //     var solution = result_set.next();
        //     System.out.printf("SOLUTION = %s \n", solution.toString());
        // }

        // for (var key : qe.getContext().keys()) {
        //     System.out.printf("QE AFTER CONTEXT %s : %s\n", key, qe.getContext().get(key));
        // }


        qe = QueryExecutionFactory.create(query, dataset);
        QC.setFactory(qe.getContext(), sageFactory);

        
        return qe;

        // return super.createQueryExecution(action, query, dataset);
    }

    @Override
    public void execPost(HttpAction action) {
        System.out.printf("SAGE-QUERY_DATASET POST\n");
        super.execPost(action);
    }
    
    
}
