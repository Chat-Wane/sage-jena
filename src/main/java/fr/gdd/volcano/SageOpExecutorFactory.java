package fr.gdd.volcano;

import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.main.OpExecutor;
import org.apache.jena.sparql.engine.main.OpExecutorFactory;



/**
 * Factory to be registered in Jena ARQ. It creates an OpExecutor for
 * Sage in charge of operations customized for pausing/resuming
 * queries.
 */
public class SageOpExecutorFactory implements OpExecutorFactory {

    public SageOpExecutorFactory() { }
    
    @Override
    public OpExecutor create(ExecutionContext execCxt) {
        return new SageOpExecutor(execCxt);
    }

    

}
