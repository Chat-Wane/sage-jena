package org.apache.jena.sparql.engine.iterator;

import fr.gdd.sage.arq.SageConstants;
import fr.gdd.sage.generics.Pair;
import fr.gdd.sage.io.SageInput;
import fr.gdd.sage.io.SageOutput;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;

public class PreemptQueryIterDefaulting extends QueryIterDefaulting {

    Integer id;
    SageInput  input;
    SageOutput output;


    public PreemptQueryIterDefaulting(QueryIterator cIter, Binding _defaultObject, ExecutionContext qCxt, Integer id) {
        super(cIter, _defaultObject, qCxt) ;
        this.id = id;
        input  = qCxt.getContext().get(SageConstants.input);
        output = qCxt.getContext().get(SageConstants.output);
    }


    @Override
    protected boolean hasNextBinding() {
        if  (System.currentTimeMillis() >= input.getDeadline() || output.size() >= input.getLimit()) {
            System.out.println("DEFAULT SAVE " + (haveReturnedSomeObject));
            this.output.save(new Pair(id, haveReturnedSomeObject));
            // Need to not return false since iterator will do it,
            // otherwise, it returns an error since it `moveToNextBinding` first then
            // check `hasNextBinding` that returns false…
            // Instead, we empty the iterator by checking all members of union.
            // return false;
        }
        return super.hasNextBinding();
    }


    public void skip(boolean to) {
        haveReturnedSomeObject = to;
    }

}
