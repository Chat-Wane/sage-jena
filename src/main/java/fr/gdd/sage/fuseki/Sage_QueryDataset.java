package fr.gdd.sage.fuseki;

import java.util.Iterator;
import java.util.Optional;

import org.apache.jena.fuseki.servlets.HttpAction;
import org.apache.jena.fuseki.servlets.SPARQL_QueryDataset;
import org.apache.jena.sparql.core.Prologue;
import org.apache.jena.sparql.resultset.SPARQLResult;

import fr.gdd.sage.arq.SageConstants;
import fr.gdd.sage.io.SageInput;
import fr.gdd.sage.jena.JenaBackend;



/**
 * The processor meant to replace the actual query of dataset. Does
 * the same job but reads and writes http header to enable
 * pausing/resuming query execution.
 */
public class Sage_QueryDataset extends SPARQL_QueryDataset {

    public Sage_QueryDataset() {}

    @Override
    protected void execute(String queryString, HttpAction action) {
        var req = action.getRequest();

        // #1 create a `SageInput` with the headers of the incoming
        // request.
        Optional<String> sageHeader = Optional.ofNullable(req.getHeader(SageConstants.input.getSymbol()));
        
        
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
        // At some point, if the saved state need to be sent in the
        // headers, it should be done here. 
        // for (var key : action.getContext().keys()) {
        //     System.out.printf("AFTER EXECUTION CONTEXT %s : %s\n", key, action.getContext().get(key));
        // }

        // SageOutput sageOutput = action.getContext().get(SageConstants.output);
        // // for (var key : sageOutput.getState().keySet()) {
        // //     System.out.printf("SAGE OUTPUT %s => %s \n", key, sageOutput.getState().get(key));
        // // }
        // if ( result.isResultSet() ) {
        //     ResponseResultSet.doResponseResultSet(action, result.getResultSet(), qPrologue);
        // } else {
        //     ServletOps.errorOccurred("Unknown or invalid result type");
        // }
        // action.setResponseHeader("TEST", "WORKS");
        super.sendResults(action, result, qPrologue);
    }
}
