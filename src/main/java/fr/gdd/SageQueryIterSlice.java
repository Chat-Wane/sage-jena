package fr.gdd;

import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.QueryIterSlice;



public class SageQueryIterSlice extends QueryIterSlice {

    public SageQueryIterSlice(QueryIterator input, long startPosition, long numItems, ExecutionContext context) {
        super(input, startPosition, numItems, context);
    }

    @Override
    protected Binding moveToNextBinding() {
        // TODO Auto-generated method stub
        return super.moveToNextBinding();
    }
    

}
