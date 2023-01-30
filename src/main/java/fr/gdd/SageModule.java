package fr.gdd;

import java.io.IOException;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.fuseki.main.sys.FusekiModule;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.web.HttpSC;



public class SageModule implements FusekiModule {

    @Override
    public String name() {
        return "Sage";
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
    public void serverAfterStarting(FusekiServer server) {
        System.out.println("Customized server start on port " + server.getHttpPort());
    }
}
