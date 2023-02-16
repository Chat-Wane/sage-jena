package fr.gdd.sage.arq;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecException;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.QueryIter1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fr.gdd.sage.io.SageOutput;



/**
 * Basic copy of
 * <https://github.com/apache/jena/blob/main/jena-arq/src/main/java/org/apache/jena/sparql/engine/iterator/QueryIterSlice.java>
 */
public class SageQueryIterSlice extends QueryIter1 {
    Logger logger = LoggerFactory.getLogger(SageQueryIterSlice.class);
    
    long count = 0;
    long limit;
    long offset;
    
    Map<Integer, VolcanoIterator> iterators_map;
    SageOutput<?> output;


    
    public SageQueryIterSlice(QueryIterator cIter, long startPosition, long numItems, ExecutionContext context,
                              Map<Integer, VolcanoIterator> iterators, SageOutput<?> output) {
        super(cIter, context);
        this.iterators_map = iterators;
        this.output = output;

        // copy from `QueryIterSlice`.
        offset = startPosition;
        if (offset == Query.NOLIMIT)
            offset = 0;

        limit = numItems;
        if (limit == Query.NOLIMIT)
            limit = Long.MAX_VALUE;

        if (limit < 0)
            throw new QueryExecException("Negative LIMIT: " + limit);
        if (offset < 0)
            throw new QueryExecException("Negative OFFSET: " + offset);

        count = 0;
        // Offset counts from 0 (the no op).
        for (int i = 0; i < offset; i++) {
            // Not subtle
            if (!cIter.hasNext()) {
                close();
                break;
            }
            cIter.next();
        }
    }

    @Override
    protected boolean hasNextBinding() {
        if ( isFinished() )
            return false;
        
        if ( ! getInput().hasNext() )
            return false ;
        
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
        return true;
    }

    @Override
    protected Binding moveToNextBinding() {
        count++;
        return getInput().nextBinding();
    }

    @Override
    protected void closeSubIterator() {
    }

    @Override
    protected void requestSubCancel() {
    }
}


