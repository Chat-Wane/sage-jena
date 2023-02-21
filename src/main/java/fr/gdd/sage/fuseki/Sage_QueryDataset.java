package fr.gdd.sage.fuseki;

import java.util.Optional;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.jena.ext.xerces.impl.dv.util.Base64;
import org.apache.jena.fuseki.servlets.HttpAction;
import org.apache.jena.fuseki.servlets.SPARQL_QueryDataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fr.gdd.sage.arq.SageConstants;
import fr.gdd.sage.io.SageInput;
import fr.gdd.sage.io.SageOutput;
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
        // #A all parameters of the execution
        Optional<String> sageInputHeader  = Optional.ofNullable(req.getHeader(SageConstants.input.getSymbol()));
        SageInput<SerializableRecord> sageInput = new SageInput<>();
        if (sageInputHeader.isPresent()) {
            ObjectMapper mapper = new ObjectMapper();
            try {
                sageInput = mapper.readValue(sageInputHeader.get(), SageInput.class);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        };

        // #B the resuming state of the query execution if present
        Optional<String> sageOutputHeader = Optional.ofNullable(req.getHeader(SageConstants.output.getSymbol()));        
        if (sageOutputHeader.isPresent()) {
            SageOutput<SerializableRecord> sagePreviousOutput = new SageOutput<>();
            byte[] decoded = Base64.decode(sageOutputHeader.get());
            sagePreviousOutput = SerializationUtils.deserialize(decoded);
            sageInput.setState(sagePreviousOutput.getState());
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
