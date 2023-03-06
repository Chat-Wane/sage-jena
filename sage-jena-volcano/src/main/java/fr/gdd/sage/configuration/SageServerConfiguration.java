package fr.gdd.sage.configuration;

import fr.gdd.sage.arq.SageConstants;
import org.apache.jena.sparql.util.Context;

import java.util.Objects;



/**
 * A basic Sage configuration that will define the conditions of query
 * execution.
 **/
public class SageServerConfiguration {

    /** The maximum number of results or random walks **/
    long limit   = Long.MAX_VALUE;
    /** The maximum duration before stopping the execution and
        returning the possibly partial results. **/
    long timeout = Long.MAX_VALUE;

    // (TODO) maybe add allowed collecting modules
    

    
    public SageServerConfiguration() { }

    public SageServerConfiguration(Context context) {
        if (Objects.nonNull(context.get(SageConstants.limit)))
            this.limit   = context.get(SageConstants.limit);

        if (Objects.nonNull(context.get(SageConstants.timeout)))
            this.timeout = context.get(SageConstants.timeout);
    }
    
    public long getLimit() {
        return limit;
    }

    public long getTimeout() {
        return timeout;
    }

    public SageServerConfiguration setLimit(int limit) {
        this.limit = limit;
        return this;
    }

    public SageServerConfiguration setTimeout(int timeout) {
        this.timeout = timeout;
        return this;
    }

}
