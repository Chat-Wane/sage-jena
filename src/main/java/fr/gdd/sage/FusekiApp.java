package fr.gdd.sage;

import org.apache.jena.fuseki.auth.Auth;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.fuseki.mgt.ActionServerStatus;
import org.apache.jena.fuseki.server.Operation;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Dataset;
import org.apache.jena.sparql.mgt.Explain.InfoLevel;
import org.apache.jena.tdb2.TDB2Factory;



/**
 * Usage example of {@link fr.gdd.sage.fuseki.SageModule} within
 * an embedded Fuseki server.
 **/
public class FusekiApp {

    public static void main( String[] args ) {
        ARQ.setExecutionLogging(InfoLevel.ALL);
        
        String uiPath = "/Users/nedelec-b-2/Downloads/apache-jena-fuseki-4.7.0/webapp";
        Dataset dataset = TDB2Factory.connectDataset("watdiv10M");
        Dataset dataset2 = TDB2Factory.connectDataset("watdiv42M");
        
        // already in META-INF/services/…FusekiModule so starts from there
        // FusekiModules.add(new SageModule());
        
        FusekiServer server = FusekiServer.create()
            // .parseConfigFile("/Users/nedelec-b-2/Downloads/apache-jena-fuseki-4.7.0/run/config.ttl")
            // .parseConfigFile("configurations/sage.ttl")
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
            .add("watdiv10M", dataset)
            .add("watdiv42M", dataset)
            // .auth(AuthScheme.BASIC)
            .addEndpoint("watdiv10M", "/watdiv10M", Operation.Query, Auth.ANY_ANON)
            .build();

        server.start();
    }
}
