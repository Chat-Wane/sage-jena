package fr.gdd.sage;

import fr.gdd.sage.datasets.Watdiv10M;
import org.apache.jena.fuseki.auth.Auth;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.fuseki.mgt.ActionServerStatus;
import org.apache.jena.fuseki.server.Operation;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Dataset;
import org.apache.jena.sparql.mgt.Explain.InfoLevel;
import org.apache.jena.tdb2.TDB2Factory;
import org.apache.logging.log4j.core.tools.picocli.CommandLine;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/**
 * A Fuseki server using Sage.
 *
 * Note: This is a usage example of {@link fr.gdd.sage.fuseki.SageModule} within
 * an embedded Fuseki server. It does not aim to be an actual server. For this,
 * you need to implement your own and register SageModule as module.
 **/
public class SageFusekiServer {

    @CommandLine.Option(names = "--database",
            description = "The path to your TDB2 database. Note: If none is set, it downloads Watdiv10M.")
    public String database;

    @CommandLine.Option(names = "--ui", description = "The path to your UI folder.")
    public String ui;

    @CommandLine.Option(names = {"-v", "--verbosity"},
            description = "The verbosity level (ALL, INFO, FINE).")
    public String verbosity;

    @CommandLine.Option(names = {"-h", "--help"}, usageHelp = true, description = "Display this help message.")
    boolean usageHelpRequested;

    public static void main( String[] args ) {
        SageFusekiServer serverOptions = new SageFusekiServer();
        new CommandLine(serverOptions).parse(args);

        if (serverOptions.usageHelpRequested) {
            CommandLine.usage(new SageFusekiServer(), System.out);
            return;
        }

        if (Objects.isNull(serverOptions.database)) {
            Watdiv10M watdiv = new Watdiv10M(Optional.empty());
            serverOptions.database = watdiv.dbPath_asStr;
        }

        switch (serverOptions.verbosity) {
            case "ALL"  -> ARQ.setExecutionLogging(InfoLevel.ALL);
            case "INFO" -> ARQ.setExecutionLogging(InfoLevel.INFO);
            case "FINE" -> ARQ.setExecutionLogging(InfoLevel.FINE);
            default     -> ARQ.setExecutionLogging(InfoLevel.NONE);
        }
        
        Dataset dataset = TDB2Factory.connectDataset(serverOptions.database);

        // already in META-INF/services/…FusekiModule so starts from there
        // FusekiModules.add(new SageModule());
        
        FusekiServer.Builder serverBuilder = FusekiServer.create()
            // .parseConfigFile("configurations/sage.ttl")
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
            .add(Path.of(serverOptions.database).getFileName().toString(), dataset)
            // .auth(AuthScheme.BASIC)
            .addEndpoint(Path.of(serverOptions.database).getFileName().toString(),
                    Path.of(serverOptions.database).getFileName().toString(),
                    Operation.Query, Auth.ANY_ANON);

        if (Objects.nonNull(serverOptions.ui)) { // add UI if need be
            serverBuilder.staticFileBase(serverOptions.ui);
        }

        FusekiServer server = serverBuilder.build();
        server.start();
    }
}
