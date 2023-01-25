package fr.gdd.volcano;

import org.apache.jena.sparql.algebra.op.OpSlice;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.tdb2.solver.OpExecutorTDB2;

import fr.gdd.common.SageOutput;



public class SageOpExecutor extends OpExecutorTDB2 {

    SageStageGenerator generator;
    SageOutput output;
    
    public SageOpExecutor(ExecutionContext execCxt, SageStageGenerator generator, SageOutput output) {
        super(execCxt);
        this.generator = generator;
        this.output = output;
    }

    @Override
    public QueryIterator execute(OpSlice opSlice, QueryIterator input) {
        QueryIterator qIter = exec(opSlice.getSubOp(), input);
        qIter = new SageQueryIterSlice(qIter, opSlice.getStart(), opSlice.getLength(), execCxt,
                                       this.generator.iterators_map, this.output);
        return qIter;
    }
    
}
