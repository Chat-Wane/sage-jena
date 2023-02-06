package fr.gdd.sage.fuseki;

import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.engine.main.StageBuilder;

import java.util.Iterator;

import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.fuseki.servlets.ActionLib;
import org.apache.jena.fuseki.servlets.HttpAction;
import org.apache.jena.fuseki.servlets.ResponseResultSet;
import org.apache.jena.fuseki.servlets.SPARQL_QueryDataset;
import org.apache.jena.fuseki.servlets.ServletOps;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Prologue;
import org.apache.jena.sparql.engine.QueryEngineFactory;
import org.apache.jena.sparql.engine.QueryEngineFactoryWrapper;
import org.apache.jena.sparql.engine.QueryEngineRegistry;
import org.apache.jena.sparql.engine.main.StageGenerator;
import org.apache.jena.sparql.engine.main.solver.StageMatchTriple;
import org.apache.jena.sparql.resultset.SPARQLResult;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.sparql.util.ContextAccumulator;

import fr.gdd.sage.arq.SageConstants;
import fr.gdd.sage.arq.SageOpExecutorFactory;
import fr.gdd.sage.arq.SageStageGenerator;
import fr.gdd.sage.interfaces.SageInput;
import fr.gdd.sage.interfaces.SageOutput;
import fr.gdd.sage.jena.JenaBackend;



/**
 * The processor meant to replace the actual query of dataset. Does
 * the same job but reads and writes http header to enable
 * pausing/resuming query execution.
 */
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

        // (TODO) add headers of request to Action's context
        Iterator<String> headers_it = req.getHeaderNames().asIterator();        
        while (headers_it.hasNext()) {
            var name = headers_it.next();
            System.out.printf("REQUEST HEADER: %s \n", name);
        }

        // Prepare sage parameters
        SageInput sageInput = new SageInput<>();
        JenaBackend backend = action.getContext().get(SageConstants.backend);
        sageInput.setBackend(backend);
        action.getContext().set(SageConstants.input, sageInput);


        action.getContext().set(SageConstants.deadline, System.currentTimeMillis() + 100000);
        
        
        for (var key : action.getContext().keys()) {
            System.out.printf("ACTION CONTEXT %s : %s\n", key, action.getContext().get(key));
        }
        
        super.execute(queryString, action);
    }

    @Override
    protected void sendResults(HttpAction action, SPARQLResult result, Prologue qPrologue) {
        for (var key : action.getContext().keys()) {
            System.out.printf("AFTER EXECUTION CONTEXT %s : %s\n", key, action.getContext().get(key));
        }

        SageOutput sageOutput = action.getContext().get(SageConstants.output);
        // for (var key : sageOutput.getState().keySet()) {
        //     System.out.printf("SAGE OUTPUT %s => %s \n", key, sageOutput.getState().get(key));
        // }

        if ( result.isResultSet() ) {
            SageResponseResultSet.doResponseResultSet(action, result.getResultSet(), qPrologue);
        } else {
            ServletOps.errorOccurred("Unknown or invalid result type");
        }
        
        action.setResponseHeader("TEST", "WORKS");
    }
    

    @Override
    public void execPost(HttpAction action) {
        System.out.printf("SAGE-QUERY_DATASET POST\n");
        super.execPost(action);
    }
    
    
}
