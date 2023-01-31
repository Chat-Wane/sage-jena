package fr.gdd;

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
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sys.JenaSubsystemLifecycle;
import org.apache.jena.web.HttpSC;



// <https://github.com/apache/jena/blob/main/jena-fuseki2/jena-fuseki-main/src/main/java/org/apache/jena/fuseki/main/sys/FusekiModule.java>
public class SageModule implements FusekiModule {

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
    public void prepare(FusekiServer.Builder builder, Set<String> datasetNames, Model configModel) {
        System.out.println("Module adds servlet");
        HttpServlet servlet = new HttpServlet() {
            @Override
            public void service(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException {
                if (req.getMethod().equalsIgnoreCase("GET")) {
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

        builder.addServlet("/extra", servlet);
    }

    @Override
    public void configured(Builder serverBuilder, DataAccessPointRegistry dapRegistry, Model configModel) {
        System.out.println("SAGE CONFIGURED");
        FusekiModule.super.configured(serverBuilder, dapRegistry, configModel);
    }

    @Override
    public void configDataAccessPoint(DataAccessPoint dap, Model configModel) {
        System.out.println("configdataAcessPoint");
    }

    @Override
    public void server(FusekiServer server) {
        System.out.println("SAGE SERVER");
        FusekiModule.super.server(server);
    }

    @Override
    public void serverBeforeStarting(FusekiServer server) {
        System.out.println("Sage server before starting");
    }
    
    @Override
    public void serverAfterStarting(FusekiServer server) {
        System.out.println("Sage Server after starting");
    }

    @Override
    public void serverStopped(FusekiServer server) {
        System.out.println("SAGE Server STOPPED");
            FusekiModule.super.serverStopped(server);
    }

    @Override
    public void stop() {
        System.out.println("SAGE STOP");
        FusekiModule.super.stop();
    }

    @Override
    public int level() {
        return 9999;
    }
}
