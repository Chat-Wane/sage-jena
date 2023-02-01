package fr.gdd;


import java.io.IOException;

import javax.servlet.FilterConfig;

import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.atlas.web.AuthScheme;
import org.apache.jena.fuseki.auth.Auth;
import org.apache.jena.fuseki.cmd.JettyFusekiWebapp;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.fuseki.main.cmds.FusekiMain;
import org.apache.jena.fuseki.main.sys.FusekiModule;
import org.apache.jena.fuseki.main.sys.FusekiModules;
import org.apache.jena.fuseki.mgt.ActionDatasets;
import org.apache.jena.fuseki.mgt.ActionServerStatus;
import org.apache.jena.fuseki.server.DataService;
import org.apache.jena.fuseki.server.Operation;
import org.apache.jena.fuseki.server.OperationRegistry;
import org.apache.jena.fuseki.servlets.ActionService;
import org.apache.jena.fuseki.servlets.SPARQL_QueryGeneral;
import org.apache.jena.fuseki.system.FusekiLogging;
import org.apache.jena.fuseki.validation.QueryValidator;
import org.apache.jena.fuseki.webapp.FusekiEnv;
import org.apache.jena.fuseki.webapp.ShiroEnvironmentLoader;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.AnonId;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.engine.main.StageBuilder;
import org.apache.jena.sparql.engine.main.StageGenerator;
import org.apache.jena.sys.JenaSystem;
import org.apache.jena.tdb2.TDB2Factory;
import org.apache.shiro.config.IniSecurityManagerFactory;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.util.Factory;
import org.apache.shiro.web.config.ShiroFilterConfiguration;
import org.apache.shiro.web.env.EnvironmentLoaderListener;
import org.apache.shiro.web.env.ResourceBasedWebEnvironment;
import org.apache.shiro.web.servlet.ShiroFilter;
import org.eclipse.jetty.security.RoleInfo;
import org.eclipse.jetty.security.SecurityHandler;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.UserIdentity;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import fr.gdd.jena.JenaBackend;
import fr.gdd.volcano.SageOpExecutorFactory;
import fr.gdd.volcano.SageStageGenerator;



public class FusekiApp {

    public static void main( String[] args ) {
        String ui = "/Users/nedelec-b-2/Downloads/apache-jena-fuseki-4.7.0/webapp";
        // "/Users/nedelec-b-2/Desktop/Projects/jena/jena-fuseki2/jena-fuseki-ui/target/webapp";
        
        String datasetPath = "/Users/nedelec-b-2/Desktop/Projects/preemptable-blazegraph/watdiv10M";
        Dataset dataset = TDB2Factory.connectDataset(datasetPath);
        // dataset.begin() in backend 
        JenaBackend backend = new JenaBackend(datasetPath);
        
        // already in META-INF/services/…FusekiModule so starts from there
        // FusekiModules.add(new SageModule());

        
        StageGenerator parent = (StageGenerator) ARQ.getContext().get(ARQ.stageGenerator) ;
        SageStageGenerator sageStageGenerator = new SageStageGenerator(parent, backend);
        StageBuilder.setGenerator(ARQ.getContext(), sageStageGenerator);
        
        SageOpExecutorFactory sageFactory = new SageOpExecutorFactory();
        QC.setFactory(ARQ.getContext(), sageFactory);

        

        
        FusekiServer server = FusekiServer.create()
            // .parseConfigFile("/Users/nedelec-b-2/Downloads/apache-jena-fuseki-4.7.0/run/config.ttl")
            .staticFileBase(ui)
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

        


        
        // Handler handler = server.getJettyServer().getHandler();
        // ServletContextHandler sch =  (ServletContextHandler)handler;
        // ServletHandler servletHander = sch.getServletHandler() ;

        // ServletHolder[] holder = servletHander.getServlets();
        // for (int i = 0; i < holder.length; ++i) {
        //     System.out.printf("name:  %s \n", holder[i].getName());
        // }

        ActionService as3 =  server.getOperationRegistry().findHandler(Operation.Query);
        System.out.printf("AS3 QUERY : %s\n", as3.toString());

    }
}
