package fr.gdd.sage.fuseki;

import static java.lang.String.format;
import static org.apache.jena.riot.WebContent.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import javax.servlet.http.HttpServletRequest;

import org.apache.jena.atlas.lib.StrUtils;
import org.apache.jena.atlas.web.AcceptList;
import org.apache.jena.atlas.web.MediaType;
import org.apache.jena.fuseki.DEF;
import org.apache.jena.fuseki.FusekiException;
import org.apache.jena.fuseki.servlets.HttpAction;
import org.apache.jena.fuseki.servlets.ServletOps;
import org.apache.jena.fuseki.system.ConNeg;
import org.apache.jena.query.QueryCancelledException;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.WebContent;
import org.apache.jena.riot.resultset.ResultSetWriterRegistry;
import org.apache.jena.riot.rowset.RowSetWriterFactory;
import org.apache.jena.riot.rowset.RowSetWriterRegistry;
import org.apache.jena.riot.rowset.rw.RowSetWriterXML;
import org.apache.jena.sparql.core.Prologue;
import org.apache.jena.sparql.resultset.ResultsWriter;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.web.HttpSC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fr.gdd.sage.ReflectionUtils;
import fr.gdd.sage.arq.SageConstants;
import fr.gdd.sage.interfaces.SageOutput;

/** This is the content negotiation for each kind of SPARQL query result */
public class SageResponseResultSet
{
    private static Logger xlog = LoggerFactory.getLogger(SageResponseResultSet.class);

    // Short names for "output="
    private static final String contentOutputJSON          = "json";
    private static final String contentOutputXML           = "xml";
    private static final String contentOutputSPARQL        = "sparql";
    private static final String contentOutputText          = "text";
    private static final String contentOutputCSV           = "csv";
    private static final String contentOutputTSV           = "tsv";
    private static final String contentOutputThrift        = "thrift";


    private static Class responseOps = ReflectionUtils._getClass("org.apache.jena.fuseki.servlets.ResponseOps");
    private static Method putMethod = ReflectionUtils._getMethod(responseOps, "put", Map.class,
                                                                 String.class, String.class);
    
    public static Map<String, String> shortNamesResultSet = new HashMap<>();
    static {
        
        // Some short names.  keys are lowercase.
        ResponseOps.put(shortNamesResultSet, contentOutputJSON,   contentTypeResultsJSON);
        // ReflectionUtils._callMethod(putMethod, responseOps, null, shortNamesResultSet, contentOutputJSON,   contentTypeResultsJSON);
        ResponseOps.put(shortNamesResultSet, contentOutputSPARQL, contentTypeResultsXML);
        // ReflectionUtils._callMethod(putMethod, responseOps, null, shortNamesResultSet, contentOutputSPARQL, contentTypeResultsXML);
        ResponseOps.put(shortNamesResultSet, contentOutputXML,    contentTypeResultsXML);
        // ReflectionUtils._callMethod(putMethod, responseOps, null, shortNamesResultSet, shortNamesResultSet, contentOutputXML,    contentTypeResultsXML);
        ResponseOps.put(shortNamesResultSet, contentOutputText,   contentTypeTextPlain);
        // ReflectionUtils._callMethod(putMethod, responseOps, null, shortNamesResultSet, contentOutputText,   contentTypeTextPlain);
        ResponseOps.put(shortNamesResultSet, contentOutputCSV,    contentTypeTextCSV);
        // ReflectionUtils._callMethod(putMethod, responseOps, null, shortNamesResultSet, contentOutputCSV,    contentTypeTextCSV);
        ResponseOps.put(shortNamesResultSet, contentOutputTSV,    contentTypeTextTSV);
        // ReflectionUtils._callMethod(putMethod, responseOps, null, shortNamesResultSet, contentOutputTSV,    contentTypeTextTSV);
        ResponseOps.put(shortNamesResultSet, contentOutputThrift, contentTypeResultsThrift);
        // ReflectionUtils._callMethod(putMethod, responseOps, null, shortNamesResultSet, contentOutputThrift, contentTypeResultsThrift);
    }

    interface OutputContent { void output(OutputStream out) throws IOException; }

    public static void doResponseResultSet(HttpAction action, Boolean booleanResult) {
        doResponseResultSet$(action, null, booleanResult, null, DEF.rsOfferBoolean);
    }

    public static void doResponseResultSet(HttpAction action, ResultSet resultSet, Prologue qPrologue) {
        doResponseResultSet$(action, resultSet, null, qPrologue, DEF.rsOfferTable);
    }

    // One or the other argument must be null
    private static void doResponseResultSet$(HttpAction action,
                                             ResultSet resultSet, Boolean booleanResult,
                                             Prologue qPrologue, AcceptList contentTypeOffer) {
        HttpServletRequest request = action.getRequest();
        long id = action.id;

        if ( resultSet == null && booleanResult == null ) {
            xlog.warn("doResponseResult: Both result set and boolean result are null");
            throw new FusekiException("Both result set and boolean result are null");
        }

        if ( resultSet != null && booleanResult != null ) {
            xlog.warn("doResponseResult: Both result set and boolean result are set");
            throw new FusekiException("Both result set and boolean result are set");
        }

        String mimeType = null;
        // -- Conneg
        MediaType i = ConNeg.chooseContentType(request, contentTypeOffer, DEF.acceptRSXML);
        if ( i != null )
            mimeType = i.getContentTypeStr();

        // -- Override content type from conneg.
        // Does &output= override?
        // Requested output type by the web form or &output= in the request.
        String outputField = ResponseOps.paramOutput(request, shortNamesResultSet);    // Expands short names
        if ( outputField != null )
            mimeType = outputField;

        String serializationType = mimeType;           // Choose the serializer based on this.
        String contentType = mimeType;                 // Set the HTTP response header to this.

        // -- Stylesheet - change to application/xml.
        final String stylesheetURL = ResponseOps.paramStylesheet(request);
        if ( stylesheetURL != null && Objects.equals(serializationType,contentTypeResultsXML) )
            contentType = contentTypeXML;

        // Force to text/plain?
        String forceAccept = ResponseOps.paramForceAccept(request);
        if ( forceAccept != null )
            contentType = contentTypeTextPlain;

        // Some kind of general dispatch is neater but there are quite a few special cases.
        // text/plain is special because there is no ResultSetWriter for it (yet).
        // Text plain is special because of the formatting by prologue.
        // text/plain is not a registered result set language.
        //
        // JSON is special because of ?callback
        //
        // XML is special because of
        // (1) charset is a feature of XML, not the response
        // (2) ?stylesheet=
        //
        // Thrift is special because
        // (1) charset is meaningless
        // (2) there is no boolean result form.

        if ( Objects.equals(serializationType, contentTypeTextPlain) ) {
            textOutput(action, contentType, resultSet, qPrologue, booleanResult);
            return;
        }

        Lang lang = WebContent.contentTypeToLangResultSet(serializationType);
        if (lang == null )
            ServletOps.errorBadRequest("Not recognized for SPARQL results: "+serializationType);
        if ( ! ResultSetWriterRegistry.isRegistered(lang) )
            ServletOps.errorBadRequest("No results writer for "+serializationType);

        Context cxt = action.getContext().copy();
        String charset = charsetUTF8;
        String jsonCallback = null;

        if ( Objects.equals(serializationType, contentTypeResultsXML) ) {
            charset = null;
            if ( stylesheetURL != null )
                cxt.set(RowSetWriterXML.xmlStylesheet, stylesheetURL);
        }
        if ( Objects.equals(serializationType, contentTypeResultsJSON) ) {
            jsonCallback = ResponseOps.paramCallback(action.getRequest());
        }
        if (Objects.equals(serializationType, WebContent.contentTypeResultsThrift) ) {
            if ( booleanResult != null )
                ServletOps.errorBadRequest("Can't write a boolean result in thrift");
            charset = null;
        }
        if (Objects.equals(serializationType, WebContent.contentTypeResultsProtobuf) ) {
            if ( booleanResult != null )
                ServletOps.errorBadRequest("Can't write a boolean result in protobuf");
            charset = null;
        }


        // Finally, the general case
        generalOutput(action, lang, contentType, charset, cxt, jsonCallback, resultSet, booleanResult);
    }

    private static void textOutput(HttpAction action, String contentType, ResultSet resultSet, Prologue qPrologue, Boolean booleanResult) {
        System.out.println("NOT STREAMING");
        // Text is not streaming.
        OutputContent proc = out -> {
            if ( resultSet != null )
                ResultSetFormatter.out(out, resultSet, qPrologue);
            if (  booleanResult != null )
                ResultSetFormatter.out(out, booleanResult.booleanValue());
        };

        output(action, contentType, charsetUTF8, proc);
    }

    /** Any format */
    private static void generalOutput(HttpAction action, Lang rsLang,
                                      String contentType, String charset,
                                      Context context, String callback,
                                      ResultSet resultSet, Boolean booleanResult) {
        System.out.println("STREAMING");
        ResultsWriter rw = ResultsWriter.create()
            .lang(rsLang)
            .context(context)
            .build();

        OutputContent proc = (out) -> {
            if ( callback != null ) {
                String callbackFunction = callback;
                callbackFunction = callbackFunction.replace("\r", "");
                callbackFunction = callbackFunction.replace("\n", "");
                out.write(StrUtils.asUTF8bytes(callbackFunction));
                out.write('('); out.write('\n');
            }
            if ( resultSet != null ) {
                rw.write(out, resultSet);
                System.out.println("WRITE");
                RowSetWriterFactory factory = RowSetWriterRegistry.getFactory(rsLang);
                System.out.printf("ROWSET FACTORY %s WITH LANG %s\n", factory, rsLang);
            }
            if ( booleanResult != null )
                rw.write(out, booleanResult.booleanValue());
            if ( callback != null ) {
                out.write(')'); out.write('\n');
            }
        };
        output(action, contentType, charset, proc);
    }

    // Set HTTP response and execute OutputContent inside try-catch.
    private static void output(HttpAction action, String contentType, String charset, OutputContent proc) {
        try {
            ResponseOps.setHttpResponse(action, contentType, charset);
            ServletOps.success(action);
            OutputStream out = action.getResponseOutputStream();
            try {
                proc.output(out);

                // Patching end of execution to push uptodate headers
                // to the client.  Find out a more generic way to do
                // that without changing all signatures.
                for (var key : action.getContext().keys()) {
                    System.out.printf("ACTION CONTEXT %s : %s\n", key, action.getContext().get(key));
                }

                String ugly = "";
                SageOutput sageOutput = action.getContext().get(SageConstants.output);
                for (var key : sageOutput.getState().keySet()) {
                    ugly += key +" => "+ sageOutput.getState().get(key) + " ; ";
                    System.out.printf("SAGE OUTPUT %s => %s \n", key, sageOutput.getState().get(key));
                }
                action.setResponseHeader(SageConstants.output.toString(), ugly);

                // ByteArrayOutputStream baos = new ByteArrayOutputStream();
                // byte[] serialized = null;
                // try (ObjectOutputStream ois = new ObjectOutputStream(baos)) {
                //     ois.writeObject(sageOutput);
                //     serialized = baos.toByteArray();
                // } catch (IOException ioe) {
                //     ioe.printStackTrace();
                // }
                // action.setResponseHeader(SageConstants.output.toString(), serialized.toString());

                
                
                
                out.flush();
            } catch (QueryCancelledException ex) {
                // Status code 200 may have already been sent.
                // We can try to set the HTTP response code anyway.
                // Breaking the results is the best we can do to indicate the timeout.
                action.setResponseStatus(HttpSC.BAD_REQUEST_400);
                action.log.info(format("[%d] Query Cancelled - results truncated (but 200 may have already been sent)", action.id));
                PrintStream ps = new PrintStream(out);
                ps.println();
                ps.println("##  Query cancelled due to timeout during execution   ##");
                ps.println("##  ****          Incomplete results           ****   ##");
                ps.flush();
                out.flush();
                // No point raising an exception - 200 was sent already.
                //errorOccurred(ex);
            }
        // Includes client gone.
        } catch (IOException ex) { ServletOps.errorOccurred(ex); }
        // Do not call httpResponse.flushBuffer() at this point. JSON callback closing details haven't been added.
        // Jetty closes the stream if it is a gzip stream.
    }
}
