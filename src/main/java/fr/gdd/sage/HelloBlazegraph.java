package fr.gdd;

import java.io.File;
import java.net.URL;
import java.util.Properties;

import org.openrdf.OpenRDFException;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.rio.RDFFormat;

import com.bigdata.journal.Options;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;



public class HelloBlazegraph {
    public static void main(String[] args) throws OpenRDFException {

        final Properties props = new Properties();
        props.put(Options.BUFFER_MODE, "DiskRW"); // persistent file system located journal
        props.put(Options.FILE, "/tmp/blazegraph/test.jnl"); // journal file location

        final BigdataSail sail = new BigdataSail(props); // instantiate a sail
        final Repository repo = new BigdataSailRepository(sail); // create a Sesame repository

        repo.initialize();

        RepositoryConnection cxn ; //= repo.getConnection();
        if (repo instanceof BigdataSailRepository) {
            cxn = ((BigdataSailRepository) repo).getReadOnlyConnection();
        } else {
            cxn = repo.getConnection();
        }

        // evaluate sparql query
        try {
            
            final TupleQuery tupleQuery = cxn
                .prepareTupleQuery(QueryLanguage.SPARQL,
                                   "select ?s where { ?s ?p ?o . }");
            TupleQueryResult result = tupleQuery.evaluate();
            try {
                while (result.hasNext()) {
                    BindingSet bindingSet = result.next();
                    System.err.println(bindingSet);
                }
            } finally {
                result.close();
            }
            
        } finally {
            // close the repository connection
            cxn.close();
        }


        System.exit(1);


        
        
        try {
            // prepare a statement
            // URIImpl subject = new URIImpl("http://blazegraph.com/Blazegraph");
            // URIImpl predicate = new URIImpl("http://blazegraph.com/says");
            // Literal object = new LiteralImpl("hello meow");
            // Statement stmt = new StatementImpl(subject, predicate, object);

            // open repository connection
            //RepositoryConnection cxn = repo.getConnection();
            cxn = repo.getConnection();
            // upload data to repository
            try {
                cxn.begin();
                try {
                    cxn.add(new File("/Users/nedelec-b-2/Desktop/Projects/sage-benchmark/datasets/watdiv/watdiv.10M.nt"),
                            "http://example.com/default", RDFFormat.N3);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                // cxn.add(stmt);
                cxn.commit();
            } catch (OpenRDFException ex) {
                cxn.rollback();
                throw ex;
            } finally {
                // close the repository connection
                cxn.close();
            }

            // open connection
            if (repo instanceof BigdataSailRepository) {
                cxn = ((BigdataSailRepository) repo).getReadOnlyConnection();
            } else {
                cxn = repo.getConnection();
            }

            // evaluate sparql query
            try {

                final TupleQuery tupleQuery = cxn
                        .prepareTupleQuery(QueryLanguage.SPARQL,
                                "select ?p ?o where { <http://blazegraph.com/Blazegraph> ?p ?o . }");
                TupleQueryResult result = tupleQuery.evaluate();
                try {
                    while (result.hasNext()) {
                        BindingSet bindingSet = result.next();
                        System.err.println(bindingSet);
                    }
                } finally {
                    result.close();
                }

            } finally {
                // close the repository connection
                cxn.close();
            }

        } finally {
            repo.shutDown();
        }
    }
}
