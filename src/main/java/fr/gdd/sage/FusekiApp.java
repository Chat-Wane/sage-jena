package fr.gdd.sage;

import org.apache.jena.fuseki.auth.Auth;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.fuseki.mgt.ActionServerStatus;
import org.apache.jena.fuseki.server.Operation;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Dataset;
import org.apache.jena.sparql.mgt.Explain.InfoLevel;
import org.apache.jena.tdb2.TDB2Factory;



public class FusekiApp {

    public static void main( String[] args ) {
        ARQ.setExecutionLogging(InfoLevel.ALL);
        
        String uiPath = "/Users/nedelec-b-2/Downloads/apache-jena-fuseki-4.7.0/webapp";
        String datasetPath = "/Users/nedelec-b-2/Desktop/Projects/preemptable-blazegraph/watdiv10M";
        Dataset dataset = TDB2Factory.connectDataset(datasetPath);
        
        // already in META-INF/services/…FusekiModule so starts from there
        // FusekiModules.add(new SageModule());
        
        FusekiServer server = FusekiServer.create()
            // .parseConfigFile("/Users/nedelec-b-2/Downloads/apache-jena-fuseki-4.7.0/run/config.ttl")
            .staticFileBase(uiPath)
            .enablePing(true)
            .enableCompact(true)
            // .enableCors(true)
            .enableStats(true)
            .enableTasks(true)
            .enableMetrics(true)
            .numServerThreads(1, 10)
            // .loopback(false)
            .serverAuthPolicy(Auth.ANY_ANON)
            .addProcessor("/$/server", new ActionServerStatus())
            //.addProcessor("/$/datasets/*", new ActionDatasets())
            .add("meow", dataset)
            // .auth(AuthScheme.BASIC)
            .addEndpoint("meow", "/woof", Operation.Query, Auth.ANY_ANON)
            .build();

        server.start();
    }
}
