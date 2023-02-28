package org.apache.jena.sparql.engine.iterator;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fr.gdd.sage.arq.SageConstants;
import fr.gdd.sage.arq.VolcanoIterator;
import fr.gdd.sage.io.SageOutput;



/**
 * A slice iterator that, when the limit is reached, save the state of
 * iterators in a context output.
 **/
public class PreemptQueryIterSlice extends QueryIterSlice {
    Logger logger = LoggerFactory.getLogger(PreemptQueryIterSlice.class);
    
    Map<Integer, VolcanoIterator> iterators_map;
    SageOutput<?> output;


    
    public PreemptQueryIterSlice(QueryIterator cIter,
                                 long startPosition,
                                 long numItems,
                                 ExecutionContext context) {
        super(cIter, startPosition, numItems, context);
        this.iterators_map = context.getContext().get(SageConstants.iterators);
        this.output = context.getContext().get(SageConstants.output);
    }

    /**
     * Two changes: It saves when the limit is reached, and the order
     * of checks is not identical to that of {@link QueryIterSlice}.
     **/
    @Override
    protected boolean hasNextBinding() {
        if (isFinished()) {
            return false;
        }
        
        if ( count >= limit ) {
            Entry<Integer, VolcanoIterator> lastKey = null;
            // (TODO) Double check not necessarily .current() or .previous()
            // with volcano model.
            for (Entry<Integer, VolcanoIterator> e : this.iterators_map.entrySet()) {
                lastKey = e;

                var toSave = new Pair(e.getKey(), e.getValue().wrapped.previous());
                this.output.addState(toSave);
            }
            // (TODO) this is a ugly way to say that the last iterator
            // before the projection should save its current instead
            // of previous. However, this only work because iterators
            // are sorted and there is only one projection. etc. So we
            // need to have a datastructure to extract exactly what is
            // needed by a operator that saves.
            if (lastKey != null) {
                var toSave = new Pair(lastKey.getKey(), lastKey.getValue().wrapped.previous());
                this.output.addState(toSave);
            }
            logger.info("Pausing query execution in slice operator.");
            
            return false;
        }

        // The order is important: in case `hasNext` is infinite,
        // (e.g. with random walks) yet the threshold number of
        // elements is reached.
        if (!getInput().hasNext()) {
            return false;
        }
        
        return true;
    }
    
}


