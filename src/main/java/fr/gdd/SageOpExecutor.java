package fr.gdd;

import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpSlice;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.iterator.QueryIterSlice;
import org.apache.jena.sparql.engine.main.OpExecutor;
import org.apache.jena.sparql.engine.main.OpExecutorFactory;
import org.apache.jena.tdb2.solver.OpExecutorTDB2;



public class SageOpExecutor extends OpExecutorTDB2 {

    SageStageGenerator generator;
    
    public SageOpExecutor(ExecutionContext execCxt, SageStageGenerator generator) {
        super(execCxt);
        this.generator = generator;
    }

    @Override
    public QueryIterator execute(OpSlice opSlice, QueryIterator input) {
        System.out.println("SLIIIIIICE");
        QueryIterator qIter = exec(opSlice.getSubOp(), input);
        qIter = new SageQueryIterSlice(qIter, opSlice.getStart(), opSlice.getLength(), execCxt, this.generator.iterators);
        return qIter;
    }
    
}
