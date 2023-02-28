package org.apache.jena.sparql.engine.iterator;

import org.apache.jena.sparql.engine.ExecutionContext;



public class RandomQueryIterConcat extends QueryIterConcat {

    public RandomQueryIterConcat( ExecutionContext context) {
        super(context);
    }

    @Override
    protected boolean hasNextBinding() {
        if (isFinished()) {
            return false;
        }
        int randomIndex = (int) (Math.random()*super.iteratorList.size());
        currentQIter = iteratorList.get(randomIndex);
        return true; // to test
    }

    

}
