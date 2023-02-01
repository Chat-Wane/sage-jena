package fr.gdd.server;

import java.io.IOException;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.jena.cmd.ArgModuleGeneral;
import org.apache.jena.fuseki.build.FusekiExt;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.fuseki.main.FusekiServer.Builder;
import org.apache.jena.fuseki.main.cmds.FusekiMain;
import org.apache.jena.fuseki.main.sys.FusekiModule;
import org.apache.jena.fuseki.main.sys.FusekiModuleStep;
import org.apache.jena.fuseki.main.sys.FusekiModules;
import org.apache.jena.fuseki.server.DataAccessPoint;
import org.apache.jena.fuseki.server.DataAccessPointRegistry;
import org.apache.jena.fuseki.server.Endpoint;
import org.apache.jena.fuseki.server.Operation;
import org.apache.jena.fuseki.server.OperationRegistry;
import org.apache.jena.fuseki.servlets.ActionService;
import org.apache.jena.fuseki.system.FusekiLogging;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sys.JenaSubsystemLifecycle;
import org.apache.jena.web.HttpSC;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import fr.gdd.server.Sage_QueryDataset;



// <https://github.com/apache/jena/blob/main/jena-fuseki2/jena-fuseki-main/src/main/java/org/apache/jena/fuseki/main/sys/FusekiModule.java>
public class SageModule implements FusekiModule {

    public static boolean first = true;
    
    public SageModule() {}
    
    @Override
    public String name() {
        return "Sage";
    }
    
    @Override
    public void start() {
        System.out.println("SAGE MODULE START");
    }

    @Override
    public void prepare(Builder builder, Set<String> datasetNames, Model configModel) {
        System.out.println("SAGE ADD SERVLET PREPARE");
        HttpServlet servlet = new HttpServlet() {
            @Override
            public void service(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException {
                if (req.getMethod().equalsIgnoreCase("GET") | req.getMethod().equalsIgnoreCase("POST")) {
                    doPatch(req, res);
                    return;
                }
                super.service(req, res);
            }

            private void doPatch(HttpServletRequest req, HttpServletResponse res) throws IOException {
                // String x = IO.readWholeFileAsUTF8(req.getInputStream());
                System.out.println("HTTP PATCH: ");
                res.setStatus(HttpSC.OK_200);
            }
        };
        
        for (String datasetName : datasetNames) {
            System.out.printf("Dataset %s\n", datasetName);
            
            // builder.addServlet(datasetName+"/sparql", servlet);
            // builder.addServlet(datasetName+"/query", servlet);
        }
        
        builder.addServlet("/extra", servlet);
    }

    @Override
    public void configured(Builder serverBuilder, DataAccessPointRegistry dapRegistry, Model configModel) {
        System.out.println("SAGE CONFIGURED");
        dapRegistry.accessPoints().forEach(accessPoint->configDataAccessPoint(accessPoint, configModel));
        // FusekiModule.super.configured(serverBuilder, dapRegistry, configModel);
    }

    @Override
    public void configDataAccessPoint(DataAccessPoint dap, Model configModel) {
        System.out.println("SAGE CONFIG DATA ACCESS POINT");
        System.out.printf("DATA ACCESS POINT %s\n", dap.getName());
    }

    /**
     * Built, not started, about to be returned to the builder caller.
     */
    @Override
    public void server(FusekiServer server) {
        System.out.println("SAGE SERVER");
        FusekiModule.super.server(server);
    }

    /**
     * Server starting - called just before server.start happens.
     */
    @Override
    public void serverBeforeStarting(FusekiServer server) {
        System.out.println("SAGE BEFORE STARTING");

        

        var dapr = server.getDataAccessPointRegistry();
        for (var dap : dapr.accessPoints()) {

            
            for (var op : dap.getDataService().getOperations()) {
                System.out.printf("op %s \n", op.getDescription());
            }
            
            ActionService as =  server.getOperationRegistry().findHandler(Operation.Query);
            System.out.printf("AS QUERY  : %s\n", as.toString());
            server.getOperationRegistry().register(Operation.Query, new Sage_QueryDataset());
            ActionService as2 =  server.getOperationRegistry().findHandler(Operation.Query);
            System.out.printf("AS2 QUERY : %s\n", as2.toString());

            for (Endpoint ep : dap.getDataService().getEndpoints(Operation.Query)) {
                   ep.setProcessor(server.getOperationRegistry().findHandler(ep.getOperation()));
            }
        }


        System.out.println("---------");

        
        
        var meowPoint = server.getDataAccessPointRegistry().accessPoints().get(0);
        
        Handler handler = server.getJettyServer().getHandler();
        ServletContextHandler sch =  (ServletContextHandler)handler;
        ServletHandler servletHander = sch.getServletHandler() ;

        ServletHolder[] holder = servletHander.getServlets();
        for (int i = 0; i < holder.length; ++i) {
            System.out.printf("name:  %s \n", holder[i].getName());
        }
        
        for (var mapping : servletHander.getServletMappings()) {
            System.out.printf("MAPPING : %s\n",  mapping.toString());
        }
        
    }

    /**
     * Server started - called just after server.start happens, and before server
     * .start() returns to the application.
     */
    @Override
    public void serverAfterStarting(FusekiServer server) {
        System.out.println("SAGE AFTER STARTING");
    }

    @Override
    public void serverStopped(FusekiServer server) {
        System.out.println("SAGE STOPPED");
            FusekiModule.super.serverStopped(server);
    }

    @Override
    public void stop() {
        System.out.println("SAGE STOP");
        FusekiModule.super.stop();
    }

    @Override
    public int level() {
        System.out.println("SAGE LEVEL");
        return 999999;
    }
}
