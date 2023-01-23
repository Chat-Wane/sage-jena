package fr.gdd;

import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.main.OpExecutor;
import org.apache.jena.sparql.engine.main.OpExecutorFactory;



public class SageOpExecutorFactory implements OpExecutorFactory {

    SageStageGenerator generator;
    
    public SageOpExecutorFactory(SageStageGenerator generator) {
        this.generator = generator;
    }
    
    @Override
    public OpExecutor create(ExecutionContext execCxt) {
        return new SageOpExecutor(execCxt, generator);
    }

    

}
