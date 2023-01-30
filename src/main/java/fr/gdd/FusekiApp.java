package fr.gdd;

import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.fuseki.main.cmds.FusekiMain;
import org.apache.jena.fuseki.main.sys.FusekiModule;
import org.apache.jena.fuseki.main.sys.FusekiModules;
import org.apache.jena.fuseki.servlets.SPARQL_QueryGeneral;
import org.apache.jena.fuseki.system.FusekiLogging;
import org.apache.jena.fuseki.validation.QueryValidator;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Dataset;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.engine.main.StageBuilder;
import org.apache.jena.sparql.engine.main.StageGenerator;
import org.apache.jena.sys.JenaSystem;
import org.apache.jena.tdb2.TDB2Factory;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import fr.gdd.jena.JenaBackend;
import fr.gdd.volcano.SageOpExecutorFactory;
import fr.gdd.volcano.SageStageGenerator;



public class FusekiApp {

    public static void main( String[] args ) {
        JenaSystem.init();
        // FusekiLogging.setLogging();
        // ARQ.setExecutionLogging(InfoLevel.ALL);

        String ui = "/Users/nedelec-b-2/Desktop/Projects/jena/jena-fuseki2/jena-fuseki-ui/target/webapp";
        
        String path = "/Users/nedelec-b-2/Desktop/Projects/preemptable-blazegraph/watdiv10M";
        Dataset dataset = TDB2Factory.connectDataset(path);
        // dataset.begin();

        JenaBackend backend = new JenaBackend("/Users/nedelec-b-2/Desktop/Projects/preemptable-blazegraph/watdiv10M");
        
        StageGenerator parent = (StageGenerator)ARQ.getContext().get(ARQ.stageGenerator) ;
        SageStageGenerator sageStageGenerator = new SageStageGenerator(parent, backend);
        StageBuilder.setGenerator(ARQ.getContext(), sageStageGenerator);

        SageOpExecutorFactory sageFactory = new SageOpExecutorFactory();


        FusekiModule module = new SageModule();
        FusekiModules.add(module);
        

        // FusekiServer server = FusekiMain.build("--base="+ ui,
        //                                        "--localhost", "--port=3030",
        //                                        "--ping", "--stats", "--metrics", "--compact", "--no-cors",
        //                                        "--loc="+ path, "meow");

        FusekiServer server = FusekiServer.create()
            .add("/meow", dataset)
            .staticFileBase(ui)
            .enablePing(true)
            .enableCompact(true)
            .enableCors(true)
            .enableStats(true)
            .enableTasks(true)
            .enableMetrics(true)
            .numServerThreads(1, 10)
            .loopback(true)
            .build();
        
        System.out.printf("register %s \n", server.getOperationRegistry().toString());
        
        

        
        server.start();


        Handler handler = server.getJettyServer().getHandler();
        ServletContextHandler sch =  (ServletContextHandler)handler;
        ServletHandler servletHander = sch.getServletHandler() ;

        ServletHolder[] holder = servletHander.getServlets();
        for (int i = 0; i < holder.length; ++i) {
            System.out.printf("name:  %s \n", holder[i].getName());

        }

        Handler[] children = servletHander.getChildHandlers();
        for (int i =0; i < children.length; ++i) {
            System.out.printf("child:  %s \n", children[i].getClass().getName());
        }
        
        server.getDataAccessPointRegistry().keys().forEach((e) -> System.out.printf("dataaccesspoint %s \n", e));        
        
        
    }    
}
