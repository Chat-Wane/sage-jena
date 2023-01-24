package fr.gdd;

import org.apache.jena.dboe.base.record.Record;
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

import fr.gdd.common.SageOutput;
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

        String query_as_str = " " +
            "SELECT * WHERE {" +
            "<http://db.uwaterloo.ca/~galuc/wsdbm/City193> <http://www.geonames.org/ontology#parentCountry> ?v1." +
            "?v6 <http://schema.org/nationality> ?v1." +
            "?v6 <http://db.uwaterloo.ca/~galuc/wsdbm/likes> ?v3." +
            // "?v2 <http://purl.org/goodrelations/includes> ?v3." +
            // "?v2 <http://purl.org/goodrelations/validThrough> ?v5." +
            // "?v2 <http://purl.org/goodrelations/serialNumber> ?v4." +
            // "?v2 <http://schema.org/eligibleQuantity> ?v8." +
            // "?v6 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?v7." +
            "?v9 <http://db.uwaterloo.ca/~galuc/wsdbm/purchaseFor> ?v3." +
            // "?v2 <http://schema.org/eligibleRegion> ?v1." +
            "} LIMIT 100";
        
        // String query_as_str = "SELECT ?o WHERE {<http://db.uwaterloo.ca/~galuc/wsdbm/Retailer6> ?p ?o . ?s ?p <http://db.uwaterloo.ca/~galuc/wsdbm/Offer0> . FILTER(Regex(str(?o), 'Offer')) } LIMIT 3";
        // String query_as_str = "SELECT ?o WHERE {<http://db.uwaterloo.ca/~galuc/wsdbm/Retailer6> ?p ?o . FILTER(Regex(str(?o), 'Offer')) } LIMIT 3";
        // String query_as_str = "SELECT ?o WHERE {<http://db.uwaterloo.ca/~galuc/wsdbm/Retailer6> ?p ?o . FILTER(?o != uri('http://db.uwaterloo.ca/~galuc/wsdbm/User67267')) } ";

        Query query = QueryFactory.create(query_as_str);
        QueryExecution qe = QueryExecutionFactory.create(query, dataset.getDefaultModel());

        SageOutput output = new SageOutput<Record> ();
        SageOpExecutorFactory sageFactory = new SageOpExecutorFactory(sageStageGenerator, output);
        
        QC.setFactory(qe.getContext(), sageFactory);
        // QueryEngineRegistry reg = new QueryEngineRegistry();
        
        
        ResultSet results = qe.execSelect();
        long sum = 0;
        while (results.hasNext()) {
            System.out.printf("%s \n",results.next());
            sum+=1;
        }

        if (output.getState() != null) {
            System.out.printf("Got %s in saved state.\n", output.getState().toString());
        };
        System.out.printf("Got %s results.\n", sum);
        
        // engine.setTimeout(300); (TODO)



        // QueryExecUtils.executeQuery(query, engine) ;
    }

}
