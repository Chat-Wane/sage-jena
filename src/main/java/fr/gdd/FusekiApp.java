package fr.gdd;


import java.io.IOException;

import javax.servlet.FilterConfig;

import org.apache.jena.atlas.web.AuthScheme;
import org.apache.jena.fuseki.auth.Auth;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.fuseki.main.cmds.FusekiMain;
import org.apache.jena.fuseki.main.sys.FusekiModule;
import org.apache.jena.fuseki.main.sys.FusekiModules;
import org.apache.jena.fuseki.server.DataService;
import org.apache.jena.fuseki.server.Operation;
import org.apache.jena.fuseki.server.OperationRegistry;
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
        FusekiEnv.setEnvironment();
        JenaSystem.init();
        FusekiLogging.setLogging();
        // ARQ.setExecutionLogging(InfoLevel.ALL);

        String ui = "/Users/nedelec-b-2/Downloads/apache-jena-fuseki-4.7.0/webapp";
            // "/Users/nedelec-b-2/Desktop/Projects/jena/jena-fuseki2/jena-fuseki-ui/target/webapp";
        
        String path = "/Users/nedelec-b-2/Desktop/Projects/preemptable-blazegraph/watdiv10M";
        Dataset dataset = TDB2Factory.connectDataset(path);
        // dataset.begin();

        JenaBackend backend = new JenaBackend("/Users/nedelec-b-2/Desktop/Projects/preemptable-blazegraph/watdiv10M");
        
        StageGenerator parent = (StageGenerator)ARQ.getContext().get(ARQ.stageGenerator) ;
        SageStageGenerator sageStageGenerator = new SageStageGenerator(parent, backend);
        StageBuilder.setGenerator(ARQ.getContext(), sageStageGenerator);

        SageOpExecutorFactory sageFactory = new SageOpExecutorFactory();

        FusekiModule module = new SageModule();

        System.out.printf("FULL QUALIFIED NAME : %s\n", module.getClass().getName());
        
        FusekiModules.add(module);
        

        // FusekiServer server = FusekiMain.build("--base="+ ui,
        //                                        "--localhost", "--port=3030",
        //                                        "--ping", "--stats", "--metrics", "--compact", "--no-cors",
        //                                        "--loc="+ path, "meow");

        // Model m = ModelFactory.createDefaultModel();

        // Factory<SecurityManager> factory = new IniSecurityManagerFactory("/Users/nedelec-b-2/Downloads/apache-jena-fuseki-4.7.0/run/shiro.ini");
        // SecurityManager securityManager = factory.getInstance();

        // DatasetGraph dsg = (DatasetGraph) dataset;
        // DataService dataService = new DataService(dsg);
        // dataService.addEndpoint(OperationName.GSP_RW, "");
        // dataService.addEndpoint(OperationName.Query, "");
        // dataService.addEndpoint(OperationName.Update, "");

        var ds = DatasetFactory.createTxnMem();
        
        FusekiServer server = FusekiServer.create()
            // .parseConfigFile("/Users/nedelec-b-2/Downloads/apache-jena-fuseki-4.7.0/run/config.ttl")
            // .add("test", ds)
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
            // .add("meow", dataset)
            // .auth(AuthScheme.BASIC)
            // .addEndpoint("meow", "/woof", Operation.Query, Auth.ANY_ANON)
            .build();

        Server jettyServer = server.getJettyServer();

        ServletContextHandler servletContextHandler = (ServletContextHandler) jettyServer.getHandler();
        ServletHandler servletHandler = servletContextHandler.getServletHandler();

        // for shiro
        // ShiroEnvironmentLoader sel = new ShiroEnvironmentLoader();
        // MyShiro sel = new MyShiro();
        // EnvironmentLoaderListener ell = new EnvironmentLoaderListener();
        // System.out.printf("ENV PARAM %s\n",ell.CONFIG_LOCATIONS_PARAM);

        // servletContextHandler.addEventListener(sel);
        // servletContextHandler.addEventListener(ell);


        
        System.out.printf("register %s \n", server.getOperationRegistry().toString());

        
        
        
        server.start();
        // System.out.printf("meow     %s \n", server.datasetURL("/meow"));

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
