package fr.gdd.volcano;

import org.apache.jena.query.ARQ;
import org.apache.jena.sparql.ARQConstants;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpSlice;
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
        this.output = execCxt.getContext().get(SageStageGenerator.output);
        this.generator = execCxt.getContext().get(ARQ.stageGenerator);
    }

    @Override
    public QueryIterator execute(OpSlice opSlice, QueryIterator input) {
        QueryIterator qIter = exec(opSlice.getSubOp(), input);
        qIter = new SageQueryIterSlice(qIter, opSlice.getStart(), opSlice.getLength(), execCxt,
                                       this.generator.iterators_map, this.output);
        return qIter;
    }
    
}
