package fr.gdd.volcano;

import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.main.OpExecutor;
import org.apache.jena.sparql.engine.main.OpExecutorFactory;

import fr.gdd.common.SageOutput;



public class SageOpExecutorFactory implements OpExecutorFactory {

    SageStageGenerator generator;
    SageOutput output;
    
    public SageOpExecutorFactory(SageStageGenerator generator, SageOutput output) {
        this.generator = generator;
        this.output = output;
    }
    
    @Override
    public OpExecutor create(ExecutionContext execCxt) {
        return new SageOpExecutor(execCxt, generator, output);
    }

    

}
