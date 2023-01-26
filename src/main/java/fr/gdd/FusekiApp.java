package fr.gdd;

import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.query.Dataset;
import org.apache.jena.tdb2.TDB2Factory;



public class FusekiApp {

    public static void main( String[] args ) {
        // ARQ.setExecutionLogging(InfoLevel.ALL);
        
        String path = "/Users/nedelec-b-2/Desktop/Projects/preemptable-blazegraph/watdiv10M";
        Dataset dataset = TDB2Factory.connectDataset(path);
        // dataset.begin();

        FusekiServer server = FusekiServer.create()
            .add("/meow", dataset)
            .build();
        
        server.start();
    }    
}
