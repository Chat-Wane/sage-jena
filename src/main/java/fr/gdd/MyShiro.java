package fr.gdd;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.apache.jena.fuseki.Fuseki;
import org.apache.shiro.web.env.EnvironmentLoader;
import org.apache.shiro.web.env.ResourceBasedWebEnvironment;
import org.apache.shiro.web.env.WebEnvironment;

public class MyShiro extends EnvironmentLoader implements ServletContextListener {

    public MyShiro() {}

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        initEnvironment(sce.getServletContext());
    }


    @Override
    protected void customizeEnvironment(WebEnvironment environment) {
        if ( environment instanceof ResourceBasedWebEnvironment ) {
            ResourceBasedWebEnvironment env = (ResourceBasedWebEnvironment)environment;
            String[] locations = env.getConfigLocations();
            // String loc = huntForShiroIni(locations);

            // if (loc != null )
            locations = new String[] {"file:///Users/nedelec-b-2/Desktop/Projects/preemptable-blazegraph/temp/shiro.ini"};
            Fuseki.configLog.info("Shiro file: "+locations);
            env.setConfigLocations(locations);
        }
    }
}
