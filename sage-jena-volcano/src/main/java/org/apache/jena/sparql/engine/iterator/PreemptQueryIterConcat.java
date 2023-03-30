package org.apache.jena.sparql.engine.iterator;

import fr.gdd.sage.arq.SageConstants;
import fr.gdd.sage.generics.Pair;
import fr.gdd.sage.io.SageInput;
import fr.gdd.sage.io.SageOutput;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.Objects;

// (TODO) documentation
public class PreemptQueryIterConcat extends QueryIterConcat {

    int offset = 0;

    SageOutput output;
    SageInput sageInput;

    int id;

    public PreemptQueryIterConcat(ExecutionContext context, int id) {
        super(context);
        output = context.getContext().get(SageConstants.output);
        sageInput  = context.getContext().get(SageConstants.input);
        this.id = id;
    }

    @Override
    protected boolean hasNextBinding() {
        System.out.println("iter concat : output size"  + output.size());
        if  (System.currentTimeMillis() >= sageInput.getDeadline() || output.size() >= sageInput.getLimit()) {
            System.out.println("CONCAT  "+ id + " SAVE " + (offset));
            this.output.save(new Pair(id, offset));
            // Need to not return false since iterator will do it,
            // otherwise, it returns an error since it `moveToNextBinding` first then
            // check `hasNextBinding` that returns false…
            // Instead, we empty the iterator by checking all members of union.
            // return false;
        }

        // Copy/pasta of `hasNextBinding` with an offset increment on
        // `iterator.next`.
        if ( isFinished() )
            return false ;

        init();
        if ( currentQIter == null )
            return false ;

        while ( ! currentQIter.hasNext() )
        {
            // End sub iterator
            //currentQIter.close() ;
            currentQIter = null ;
            if ( iterator.hasNext() ) {
                offset += 1;
                currentQIter = iterator.next();
            }
            if ( currentQIter == null )
            {
                // No more.
                //close() ;
                return false ;
            }
        }

        return true;
    }


    public void skip(int to){
        System.out.println("CONCAT " + id + " Skip to " + to);
        this.offset = to;
        // init(to);
    }

    private void init(Integer... start) {
        if (!initialized) {
            currentQIter = null;
            if (iterator == null) {
                if (Objects.nonNull(start) && start.length > 0) {
                    iteratorList = iteratorList.subList(start[0], iteratorList.size());
                }
                iterator = iteratorList.listIterator();
            }
            if (iterator.hasNext())
                currentQIter = iterator.next();
            initialized = true;
        }
    }

}
