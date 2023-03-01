package fr.gdd.sage;

import java.util.Map;

import org.apache.jena.query.ARQ;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.tdb2.TDB2Factory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fr.gdd.sage.arq.SageConstants;
import fr.gdd.sage.configuration.SageInputBuilder;
import fr.gdd.sage.configuration.SageServerConfiguration;
import fr.gdd.sage.io.SageInput;
import fr.gdd.sage.io.SageOutput;



/**
 * Aims to ease the simple execution of a query from start to finish
 * despite pause/resume.
 **/
public class ExecuteUtils {
    Logger logger = LoggerFactory.getLogger(ExecuteUtils.class);
    
    public static void executeTillTheEnd(Dataset dataset, Query query, SageServerConfiguration configuration) {
        long nbPreempt = 0;
        long sum = 0;
        SageOutput<?> results = null;
        SageInput<?>  sageInput  = new SageInput<>();
            
        while (results == null || (results.getState() != null)) {
            nbPreempt += 1;
            System.out.println("RESTARTING NEW EXECUTION");
            QueryExecution qe = QueryExecutionFactory.create(query, dataset);
            System.out.printf("sage Input state : %s\n", sageInput.getState());
            qe.getContext().put(SageConstants.input, sageInput);
            qe.getContext().put(SageConstants.output, new SageOutput<>());

            ResultSet result_set = qe.execSelect();
            
            while (result_set.hasNext()) { // must enumerate to call volcano
                result_set.next();
                // System.out.printf("%s \n",result_set.next());
                sum += 1;
            }
            
            results = qe.getContext().get(SageConstants.output);
            sageInput.setState((Map)results.getState());
            System.out.println();
            System.out.printf("Saved state %s \n", results.getState());
            System.out.printf("%s results so far\n", sum);
            qe.close();
        }
    }


    public static void main(String[] args) {
        String path = "/Users/nedelec-b-2/Desktop/Projects/sage-jena/sage-jena-module/watdiv10M";
        Dataset dataset = TDB2Factory.connectDataset(path);
        dataset.begin();

        QC.setFactory(dataset.getContext(), QC.getFactory(ARQ.getContext()));
        
        String query_as_str = "SELECT ?o WHERE {<http://db.uwaterloo.ca/~galuc/wsdbm/Retailer6> ?p ?o .} LIMIT 1";
        Query query = QueryFactory.create(query_as_str);

        executeTillTheEnd(dataset, query, null);
        dataset.end();
    }
    
}
