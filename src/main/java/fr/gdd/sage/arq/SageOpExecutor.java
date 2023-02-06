package fr.gdd.sage.arq;

import java.util.Map;
import java.util.TreeMap;

import org.apache.jena.query.ARQ;
import org.apache.jena.sparql.ARQConstants;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpQuadPattern;
import org.apache.jena.sparql.algebra.op.OpSlice;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.iterator.QueryIterSlice;
import org.apache.jena.sparql.engine.main.OpExecutor;
import org.apache.jena.sparql.engine.main.OpExecutorFactory;
import org.apache.jena.sparql.util.Symbol;
import org.apache.jena.tdb2.solver.OpExecutorTDB2;

import fr.gdd.sage.interfaces.SageOutput;



/**
 * Some operators need rewriting to ensure preemptiveness. 
 */
public class SageOpExecutor extends OpExecutorTDB2 {

    SageOutput output; // where pausing state is saved when need be.
    public Map<Integer, VolcanoIterator> iterators; // all iterators that may need saving
    
    public SageOpExecutor(ExecutionContext context) {
        super(context);
        this.output = new SageOutput<>();
        execCxt.getContext().set(SageConstants.output, output);
        this.iterators = new TreeMap<Integer, VolcanoIterator>();
        execCxt.getContext().set(SageConstants.iterators, iterators);
    }

    @Override
    protected QueryIterator execute(OpBGP opBGP, QueryIterator input) {
        System.out.println("BGPBGPBGP");
        return super.execute(opBGP, input);
    }
    
    @Override
    protected QueryIterator execute(OpTriple opTriple, QueryIterator input) {
        System.out.printf("TRIPLE\n");
        return super.execute(opTriple, input);
    }
    
    @Override
    protected QueryIterator execute(OpQuadPattern quadPattern, QueryIterator input) {
        System.out.printf("QUAD %s\n", quadPattern.toString());
        BasicPattern bgp = quadPattern.getBasicPattern();
        return SageStageGenerator.executeTriplePattern(bgp, input, execCxt);
    }

    
    @Override
    public QueryIterator execute(OpSlice opSlice, QueryIterator input) {
        System.out.printf("SLICE\n");
        QueryIterator qIter = exec(opSlice.getSubOp(), input);
        qIter = new SageQueryIterSlice(qIter, opSlice.getStart(), opSlice.getLength(), execCxt,
                                       this.iterators, this.output);
        return qIter;
    }
    
}
