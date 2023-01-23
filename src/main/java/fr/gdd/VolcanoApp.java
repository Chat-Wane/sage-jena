package fr.gdd;

import org.apache.jena.graph.Graph;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryEngineRegistry;
import org.apache.jena.sparql.engine.main.OpExecutorFactory;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.engine.main.StageBuilder;
import org.apache.jena.sparql.engine.main.StageGenerator;
import org.apache.jena.sparql.mgt.Explain;
import org.apache.jena.sparql.mgt.Explain.InfoLevel;
import org.apache.jena.sparql.util.QueryExecUtils;
import org.apache.jena.tdb2.TDB2Factory;
import org.apache.jena.tdb2.store.DatasetGraphTDB;
import org.apache.jena.tdb2.sys.TDBInternal;

import fr.gdd.jena.JenaBackend;



public class VolcanoApp {

    public static void main( String[] args ) {
        ARQ.setExecutionLogging(InfoLevel.ALL);
        
        String path = "/Users/nedelec-b-2/Desktop/Projects/preemptable-blazegraph/watdiv10M";
        Dataset dataset = TDB2Factory.connectDataset(path);
        // DatasetGraphTDB graph = TDBInternal.getDatasetGraphTDB(dataset);

        JenaBackend backend = new JenaBackend("/Users/nedelec-b-2/Desktop/Projects/preemptable-blazegraph/watdiv10M");
        
        StageGenerator parent = (StageGenerator)ARQ.getContext().get(ARQ.stageGenerator) ;
        SageStageGenerator sageStageGenerator = new SageStageGenerator(parent, backend);
        
        StageBuilder.setGenerator(ARQ.getContext(), sageStageGenerator);
        // QC.setFactory(ARQ.getContext(), SageOpExecutor.factory) ;

        String query_as_str = "SELECT ?o WHERE {<http://db.uwaterloo.ca/~galuc/wsdbm/Retailer6> ?p ?o . FILTER(Regex(str(?o), 'Offer')) } LIMIT 3";
        // String query_as_str = "SELECT ?o WHERE {<http://db.uwaterloo.ca/~galuc/wsdbm/Retailer6> ?p ?o . FILTER(?o != uri('http://db.uwaterloo.ca/~galuc/wsdbm/User67267')) } ";

        Query query = QueryFactory.create(query_as_str);
        QueryExecution qe = QueryExecutionFactory.create(query, dataset.getDefaultModel());

        QC.setFactory(qe.getContext(), SageOpExecutor.factory);
        // QueryEngineRegistry reg = new QueryEngineRegistry();
        
        
        ResultSet results = qe.execSelect();
        while (results.hasNext()) {
            System.out.printf("%s \n",results.next());
        }
        
        // engine.setTimeout(300); (TODO)



        // QueryExecUtils.executeQuery(query, engine) ;
    }

}
