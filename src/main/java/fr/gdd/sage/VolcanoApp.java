package fr.gdd.sage;

import org.apache.jena.dboe.base.record.Record;
import org.apache.jena.fuseki.main.FusekiServer;
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

import fr.gdd.sage.io.SageInput;
import fr.gdd.sage.io.SageOutput;
import fr.gdd.sage.jena.JenaBackend;
import fr.gdd.sage.jena.SerializableRecord;
import fr.gdd.sage.arq.SageConstants;
import fr.gdd.sage.arq.SageOpExecutor;
import fr.gdd.sage.arq.SageStageGenerator;



public class VolcanoApp {

    public static void main( String[] args ) {
        // ARQ.setExecutionLogging(InfoLevel.ALL);

        String path = "/Users/nedelec-b-2/Desktop/Projects/preemptable-blazegraph/watdiv10M";
        Dataset dataset = TDB2Factory.connectDataset(path);
        // dataset.begin();
        
        // DatasetGraphTDB graph = TDBInternal.getDatasetGraphTDB(dataset);

        JenaBackend backend = new JenaBackend("/Users/nedelec-b-2/Desktop/Projects/preemptable-blazegraph/watdiv10M");
        
        StageGenerator parent = (StageGenerator)ARQ.getContext().get(ARQ.stageGenerator) ;
        SageStageGenerator sageStageGenerator = new SageStageGenerator(parent);
        
        StageBuilder.setGenerator(ARQ.getContext(), sageStageGenerator);
        // QC.setFactory(ARQ.getContext(), SageOpExecutor.factory) ;

        String query_as_str = " " +
            "SELECT * WHERE {" +
            "<http://db.uwaterloo.ca/~galuc/wsdbm/City193> <http://www.geonames.org/ontology#parentCountry> ?v1." +
            "?v6 <http://schema.org/nationality> ?v1." +
            "?v6 <http://db.uwaterloo.ca/~galuc/wsdbm/likes> ?v3." +
            "?v2 <http://purl.org/goodrelations/includes> ?v3." +
            "?v2 <http://purl.org/goodrelations/validThrough> ?v5." +
            "?v2 <http://purl.org/goodrelations/serialNumber> ?v4." +
            "?v2 <http://schema.org/eligibleQuantity> ?v8." +
            "?v6 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?v7." +
            "?v9 <http://db.uwaterloo.ca/~galuc/wsdbm/purchaseFor> ?v3." +
            "?v2 <http://schema.org/eligibleRegion> ?v1." +
            "} LIMIT 1000";
        
        // String query_as_str = "SELECT ?o WHERE {<http://db.uwaterloo.ca/~galuc/wsdbm/Retailer6> ?p ?o . ?s ?p <http://db.uwaterloo.ca/~galuc/wsdbm/Offer0> . FILTER(Regex(str(?o), 'Offer')) } LIMIT 3";
        // String query_as_str = "SELECT ?o WHERE {<http://db.uwaterloo.ca/~galuc/wsdbm/Retailer6> ?p ?o . FILTER(Regex(str(?o), 'Offer')) } LIMIT 3";
        // String query_as_str = "SELECT ?o WHERE {<http://db.uwaterloo.ca/~galuc/wsdbm/Retailer6> ?p ?o . FILTER(?o != uri('http://db.uwaterloo.ca/~galuc/wsdbm/User67267')) } ";

        Query query = QueryFactory.create(query_as_str);

        long nbPreempt = 0;
        long sum = 0;
        SageOutput<SerializableRecord> results = null;
        long timeout = 1; //ms
        SageInput<SerializableRecord>  input  = new SageInput<>();
        
        long startExecution = System.currentTimeMillis();
        while (results == null || (results.getState() != null && results.getState().size() > 0)) {
            nbPreempt +=1;
            // (TODO) maybe not the way to go to create another
            // queryExecution, double check
            System.out.println("RESTARTING NEW EXECUTION");
            QueryExecution qe = QueryExecutionFactory.create(query, dataset.getDefaultModel());
            qe.getContext().put(SageConstants.input, input);
            qe.getContext().put(SageConstants.deadline, System.currentTimeMillis() + timeout);
            qe.getContext().put(SageConstants.output, new SageOutput<>());
            QC.setFactory(qe.getContext(), SageOpExecutor.factory);

            // for (var key : qe.getContext().keys()) {
            //     System.out.printf("%s  => %s \n", key, qe.getContext().get(key));
            // }

            // System.exit(1);

            ResultSet result_set = qe.execSelect();
            
            while (result_set.hasNext()) { // must enumerate to call volcano
                result_set.next();
                // System.out.printf("%s \n",result_set.next());
                sum += 1;
            }


            
            results = qe.getContext().get(SageConstants.output);
            input.setState(results.getState());
            //sageStageGenerator.setSageInput(input.setState(results.getState()));
            System.out.println();
            System.out.printf("Saved state %s \n", results.getState());
            System.out.printf("%s results so far\n", sum);
            qe.close();
        };
        System.out.printf("NB PReempt = %s\n" , nbPreempt);
        System.out.printf("Took %s ms to get %s results.\n", System.currentTimeMillis() - startExecution,  sum);
        
    }

}
