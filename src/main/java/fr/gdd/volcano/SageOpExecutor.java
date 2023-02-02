package fr.gdd.volcano;

import org.apache.jena.query.ARQ;
import org.apache.jena.sparql.ARQConstants;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpQuadPattern;
import org.apache.jena.sparql.algebra.op.OpSlice;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.iterator.QueryIterSlice;
import org.apache.jena.sparql.engine.main.OpExecutor;
import org.apache.jena.sparql.engine.main.OpExecutorFactory;
import org.apache.jena.sparql.util.Symbol;
import org.apache.jena.tdb2.solver.OpExecutorTDB2;

import fr.gdd.common.SageOutput;



/**
 * Some operators need rewriting to ensure preemptiveness. 
 */
public class SageOpExecutor extends OpExecutorTDB2 {

    SageStageGenerator generator; // contains the map of all iterators.
    SageOutput output; // where pausing state is saved when need be.
    
    public SageOpExecutor(ExecutionContext execCxt) {
        super(execCxt);
        System.out.println("SAGE OP EXECTOR INIT");
        this.output = execCxt.getContext().get(SageStageGenerator.output);
        // this.generator = execCxt.getContext().get(ARQ.stageGenerator);
    }

    @Override
    protected QueryIterator execute(OpBGP opBGP, QueryIterator input) {
        System.out.println("BGPBGPBGP");
        return super.execute(opBGP, input);
    }

    @Override
    public QueryIterator executeOp(Op op, QueryIterator input) {
        System.out.printf("OP EXECUTE %s \n", op.toString());
        return super.executeOp(op, input);
    }

    @Override
    protected QueryIterator execute(OpTriple opTriple, QueryIterator input) {
        System.out.printf("TRIPLE\n");
        return super.execute(opTriple, input);
    }

    @Override
    protected QueryIterator execute(OpJoin opJoin, QueryIterator input) {
        System.out.printf("JOIN\n");
        return super.execute(opJoin, input);
    }

    @Override
    protected QueryIterator execute(OpQuadPattern quadPattern, QueryIterator input) {
        System.out.printf("QUAD\n");
        return super.execute(quadPattern, input);
    }

    
    @Override
    public QueryIterator execute(OpSlice opSlice, QueryIterator input) {
        System.out.println("SLIIIIICE");
        return super.execute(opSlice, input);
        // QueryIterator qIter = exec(opSlice.getSubOp(), input);
        // qIter = new SageQueryIterSlice(qIter, opSlice.getStart(), opSlice.getLength(), execCxt,
        //                                this.generator.iterators_map, this.output);
        // return qIter;
    }
    
}
