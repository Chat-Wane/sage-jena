package fr.gdd.sage.fuseki;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.util.Iterator;
import java.util.Optional;

import org.apache.jena.fuseki.servlets.HttpAction;
import org.apache.jena.fuseki.servlets.SPARQL_QueryDataset;
import org.apache.jena.sparql.core.Prologue;
import org.apache.jena.sparql.resultset.SPARQLResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fr.gdd.sage.arq.SageConstants;
import fr.gdd.sage.io.SageInput;
import fr.gdd.sage.jena.SerializableRecord;



/**
 * The processor meant to replace the actual query of dataset. Does
 * the same job but reads and writes http header to enable
 * pausing/resuming query execution.
 */
public class Sage_QueryDataset extends SPARQL_QueryDataset {
    Logger logger = LoggerFactory.getLogger(Sage_QueryDataset.class);

    @Override
    protected void execute(String queryString, HttpAction action) {
        var req = action.getRequest();

        // #1 create a `SageInput` with the headers of the incoming
        // request.
        Optional<String> sageHeader = Optional.ofNullable(req.getHeader(SageConstants.input.getSymbol()));

        SageInput<SerializableRecord> sageInput = new SageInput<>();
        if (sageHeader.isPresent()) {
            ByteArrayInputStream bais = new ByteArrayInputStream(sageHeader.get().getBytes());
            try {
                ObjectInput oi = new ObjectInputStream(bais);
                sageInput = (SageInput<SerializableRecord>) oi.readObject();
            } catch (IOException | ClassNotFoundException | ClassCastException e) {
                logger.warn(e.getMessage());
            }
        }

        // #2 put the deserialized input in the execution context
        action.getContext().set(SageConstants.input, sageInput);
        super.execute(queryString, action);
    }

    // If at some point, the saved state need to be sent in the
    // headers, it could be done by @overriding
    // `sendResults(HttpAction action, SPARQLResult result, Prologue
    // qPrologue)`
}
