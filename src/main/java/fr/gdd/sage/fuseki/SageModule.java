package fr.gdd.sage.fuseki;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * Module in charge of replacing fuseki's normal behavior for `query`
 * by one that enables preemptive evaluation of queries, i.e. one that
 * enables pausing/resuming of query execution on demand, depending on
 * arguments in http headers.
 * 
 * The module simply sets the processor of `Operation.QUERY` to ours,
 * for every dataset and endpoint.
 *
 * For this to work, either set full class name in a
 * `META-INF/…/…FusekiModule` file as per say in documentation, or
 * work with `addModule`.
 */
public class SageModule implements FusekiModule {
    Logger logger = LoggerFactory.getLogger(SageModule.class);
    
    public SageModule() {}
    
    @Override
    public String name() {
        return "Sage";
    }
    
    @Override
    public void start() {
        logger.info("start !");
    }

    /**
     * Server starting - called just before server.start happens.
     */
    @Override
    public void serverBeforeStarting(FusekiServer server) {
        logger.info("Patching the processor for query operations…");
        
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
    }
    
    @Override
    public void serverStopped(FusekiServer server) {
        // (TODO) maybe put back the default behavior        
    }

    @Override
    public void stop() {
        logger.info("Stop! Have a good day!");
    }

    @Override
    public int level() {
        // (TODO) find out the proper level for this module.
        return 999999;
    }
}
