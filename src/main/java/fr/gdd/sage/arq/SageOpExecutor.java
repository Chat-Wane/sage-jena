package fr.gdd.sage.arq;

import java.util.Map;
import java.util.TreeMap;

import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpQuadPattern;
import org.apache.jena.sparql.algebra.op.OpSlice;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.tdb2.solver.PatternMatchSage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.jena.tdb2.solver.OpExecutorTDB2;

import fr.gdd.sage.configuration.SageServerConfiguration;
import fr.gdd.sage.io.SageOutput;



/**
 * Some operators need rewriting to enable pausing/resuming their
 * operation.
 **/
public class SageOpExecutor extends OpExecutorTDB2 {
    static Logger log = LoggerFactory.getLogger(SageOpExecutor.class);
    
    SageOutput<?> output; // where pausing state is saved when need be.
    public Map<Integer, VolcanoIterator> iterators; // all iterators that may need saving
    SageServerConfiguration configuration;


    
    SageOpExecutor(ExecutionContext context, SageServerConfiguration configuration) {
        super(context);
        this.configuration = configuration;
        this.output = new SageOutput<>();
        execCxt.getContext().set(SageConstants.output, output);
        this.iterators = new TreeMap<Integer, VolcanoIterator>();
        execCxt.getContext().set(SageConstants.iterators, iterators);
        execCxt.getContext().set(SageConstants.scanFactory, new VolcanoIteratorFactory(context));
    }

    @Override
    protected QueryIterator execute(OpBGP opBGP, QueryIterator input) {
        System.out.println("BGPBGPBGP");
        return super.execute(opBGP, input);
    }
    
    @Override
    protected QueryIterator execute(OpTriple opTriple, QueryIterator input) {
        System.out.printf("TRIPLE\n");
        return PatternMatchSage.matchTriplePattern(opTriple.asBGP().getPattern(), input, execCxt);
    }
    
    @Override
    protected QueryIterator execute(OpQuadPattern quadPattern, QueryIterator input) {
        System.out.printf("QUAD %s\n", quadPattern.toString());
        return PatternMatchSage.matchQuadPattern(quadPattern.getBasicPattern(), quadPattern.getGraphNode(), input, execCxt);
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
