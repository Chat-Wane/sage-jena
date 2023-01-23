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

    public final static OpExecutorFactory factory = new OpExecutorFactory() {
            @Override
            public OpExecutor create(ExecutionContext execCxt){
                System.out.println("NEW SAGE OP EXEC");
                return new SageOpExecutor(execCxt);
            }
        };

    public SageOpExecutor(ExecutionContext execCxt) {
        super(execCxt);
    }

    @Override
    public QueryIterator execute(OpSlice opSlice, QueryIterator input) {
        System.out.println("SLIIIIIICE");
        QueryIterator qIter = exec(opSlice.getSubOp(), input);
        qIter = new QueryIterSlice(qIter, opSlice.getStart(), opSlice.getLength(), execCxt);
        return qIter;
    }

    // @Override
    // public QueryIterator execute(OpBGP opBPG, QueryIterator input) {
    //     System.out.println("BGP");
    //     return null;
    // }

}
