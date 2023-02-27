package fr.gdd.sage.arq;

import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.main.OpExecutor;
import org.apache.jena.sparql.engine.main.OpExecutorFactory;
import org.apache.jena.sparql.util.Context;

import fr.gdd.sage.configuration.SageServerConfiguration;



/**
 * Factory to be registered in Jena ARQ. It creates an OpExecutor for
 * Sage in charge of operations customized for pausing/resuming
 * queries.
 */
public class SageOpExecutorFactory implements OpExecutorFactory {
    SageServerConfiguration configuration;

    public SageOpExecutorFactory(Context context) {
        configuration = new SageServerConfiguration(context);
    }
    
    @Override
    public OpExecutor create(ExecutionContext context) {
        return new SageOpExecutor(context, configuration);
    }
}

