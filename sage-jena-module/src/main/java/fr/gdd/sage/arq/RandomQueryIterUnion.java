package fr.gdd.sage.arq;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.main.iterator.QueryIterUnion;



/**
 * Unions create an iterator that concatenate operation. We want this
 * iterator to be randomize, therefore, each `nextStage` randomizes
 * the list of operations. 
 **/
public class RandomQueryIterUnion extends QueryIterUnion {

    List<Op> initialOps;
    
    public RandomQueryIterUnion(QueryIterator input,
                                List<Op> subOps,
                                ExecutionContext context) {
        super(input, subOps, context);
        initialOps = new ArrayList<Op>(subOps);
    }

    @Override
    protected QueryIterator nextStage(Binding binding) {
        Collections.shuffle(initialOps);
        subOps = new ArrayList<Op>(initialOps);;
        return super.nextStage(binding);
    }
}
