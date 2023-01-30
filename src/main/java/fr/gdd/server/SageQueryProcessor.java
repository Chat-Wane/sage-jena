package fr.gdd.server;

import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.fuseki.servlets.HttpAction;
import org.apache.jena.fuseki.servlets.SPARQLQueryProcessor;
import org.apache.jena.fuseki.servlets.SPARQL_QueryDataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.sparql.core.DatasetGraph;



public class SageQueryProcessor extends SPARQL_QueryDataset {

    @Override
    protected QueryExecution createQueryExecution(HttpAction action, Query query, DatasetGraph dataset) {
        System.out.println("CARJACKED THE QUERY EXECUTION");
        return super.createQueryExecution(action, query, dataset);
    }
    
}
