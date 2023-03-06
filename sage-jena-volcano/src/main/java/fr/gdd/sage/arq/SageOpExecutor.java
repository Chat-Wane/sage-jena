package fr.gdd.sage.arq;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpQuadPattern;
import org.apache.jena.sparql.algebra.op.OpSlice;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.algebra.op.OpUnion;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.iterator.PreemptQueryIterSlice;
import org.apache.jena.sparql.engine.iterator.RandomQueryIterUnion;
import org.apache.jena.tdb2.solver.OpExecutorTDB2;
import org.apache.jena.tdb2.solver.PatternMatchSage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fr.gdd.sage.configuration.SageInputBuilder;
import fr.gdd.sage.configuration.SageServerConfiguration;
import fr.gdd.sage.io.SageInput;
import fr.gdd.sage.io.SageOutput;



/**
 * Some operators need rewriting to enable pausing/resuming their
 * operation.
 **/
public class SageOpExecutor extends OpExecutorTDB2 {
    static Logger log = LoggerFactory.getLogger(SageOpExecutor.class);
    
    SageOutput<?> output; // where pausing state is saved when need be.
    public Map<Integer, VolcanoIterator> iterators; // all iterators that may need saving


    
    SageOpExecutor(ExecutionContext context, SageServerConfiguration configuration) {
        super(context);
        
        SageInput<?> input = new SageInputBuilder()
            .globalConfig(configuration)
            .localInput(context.getContext().get(SageConstants.input))
            .build();
        
        this.output = new SageOutput<>();
        this.iterators = new TreeMap<>();
        
        execCxt.getContext().set(SageConstants.output, output);
        execCxt.getContext().set(SageConstants.input, input);
        execCxt.getContext().set(SageConstants.iterators, iterators);
        execCxt.getContext().set(SageConstants.scanFactory, new VolcanoIteratorFactory(execCxt));
    }

    @Override
    protected QueryIterator execute(OpBGP opBGP, QueryIterator input) {
        return PatternMatchSage.matchTriplePattern(opBGP.getPattern(), input, execCxt);
    }
    
    @Override
    protected QueryIterator execute(OpTriple opTriple, QueryIterator input) {
        return PatternMatchSage.matchTriplePattern(opTriple.asBGP().getPattern(), input, execCxt);
    }
    
    @Override
    protected QueryIterator execute(OpQuadPattern quadPattern, QueryIterator input) {
        return PatternMatchSage.matchQuadPattern(quadPattern.getBasicPattern(), quadPattern.getGraphNode(), input, execCxt);
    }
    
    @Override
    public QueryIterator execute(OpSlice opSlice, QueryIterator input) {
        QueryIterator qIter = exec(opSlice.getSubOp(), input);
        qIter = new PreemptQueryIterSlice(qIter, opSlice.getStart(), opSlice.getLength(), execCxt);
        return qIter;
    }


    @Override
    public QueryIterator execute(OpUnion union, QueryIterator input) {
        // (TODO) maybe RandomOpExecutor would be more appropriate.
        SageInput sageInput = execCxt.getContext().get(SageConstants.input);
        if (!sageInput.isRandomWalking()) {
            return super.execute(union, input);
        }
        // copy from `OpExecutor`
        List<Op> x = flattenUnion(union);
        QueryIterator cIter = new RandomQueryIterUnion(input, x, execCxt);
        return cIter;
    }
    
}
