package fr.gdd.sage;

import fr.gdd.sage.arq.OpExecutorSage;
import fr.gdd.sage.arq.QueryEngineSage;
import fr.gdd.sage.arq.SageConstants;
import fr.gdd.sage.datasets.Watdiv10M;
import fr.gdd.sage.io.SageInput;
import org.apache.jena.query.*;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.tdb2.TDB2Factory;
import org.apache.jena.tdb2.solver.OpExecutorTDB2;
import org.apache.jena.tdb2.solver.QueryEngineTDB;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Useful for debugging the same pattern of {@link SimplePatternBenchmark}.
 * Useful for profiling as well.
 * Indeed,`jmh` starts new VMs precluding the use of debug/profiling mode…
 */
class SimplePatternBenchmarkTest {

    static Dataset dataset;

    @BeforeAll
    public static void initialize_database() {
        Optional dirPath_opt = Optional.of("target");
        Watdiv10M watdiv = new Watdiv10M(dirPath_opt); // creates the db if need be
        dataset = TDB2Factory.connectDataset(watdiv.dbPath_asStr);
        dataset.begin(ReadWrite.READ);
    }

    @AfterAll
    public static void close_database() {
        dataset.end();
    }

    @Test
    public void execute_query_1000_with_TDB() {
        String query = "SELECT * WHERE {" +
                "<http://db.uwaterloo.ca/~galuc/wsdbm/City193> <http://www.geonames.org/ontology#parentCountry> ?v1.\n" +
                "        ?v6 <http://schema.org/nationality> ?v1.\n" +
                "        ?v6 <http://db.uwaterloo.ca/~galuc/wsdbm/likes> ?v3.\n" +
                "        ?v2 <http://purl.org/goodrelations/includes> ?v3.\n" +
                "        ?v2 <http://purl.org/goodrelations/validThrough> ?v5.\n" +
                "        ?v2 <http://purl.org/goodrelations/serialNumber> ?v4.\n" +
                "        ?v2 <http://schema.org/eligibleQuantity> ?v8.\n" +
                "        ?v6 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?v7.\n" +
                "        ?v9 <http://db.uwaterloo.ca/~galuc/wsdbm/purchaseFor> ?v3.\n" +
                "        ?v2 <http://schema.org/eligibleRegion> ?v1."+
                "}";

        QC.setFactory(dataset.getContext(), OpExecutorTDB2.OpExecFactoryTDB);
        QueryEngineTDB.register();

        Context c = dataset.getContext().copy();
        c.set(ARQ.optimization, false);

        QueryExecution queryExecution = null;
        try {
            queryExecution = QueryExecution.create()
                    .dataset(dataset)
                    .context(c)
                    .query(query).build();
        } catch (Exception e) {
            e.printStackTrace();
        }

        long nbResults = 0;
        ResultSet rs = queryExecution.execSelect() ;
        while (rs.hasNext()) {
            rs.next();
            nbResults+=1;
        }

    }

    @Test
    public void execute_query_1000_with_Sage() {
        String query = "SELECT * WHERE {" +
                "<http://db.uwaterloo.ca/~galuc/wsdbm/City193> <http://www.geonames.org/ontology#parentCountry> ?v1.\n" +
                "        ?v6 <http://schema.org/nationality> ?v1.\n" +
                "        ?v6 <http://db.uwaterloo.ca/~galuc/wsdbm/likes> ?v3.\n" +
                "        ?v2 <http://purl.org/goodrelations/includes> ?v3.\n" +
                "        ?v2 <http://purl.org/goodrelations/validThrough> ?v5.\n" +
                "        ?v2 <http://purl.org/goodrelations/serialNumber> ?v4.\n" +
                "        ?v2 <http://schema.org/eligibleQuantity> ?v8.\n" +
                "        ?v6 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?v7.\n" +
                "        ?v9 <http://db.uwaterloo.ca/~galuc/wsdbm/purchaseFor> ?v3.\n" +
                "        ?v2 <http://schema.org/eligibleRegion> ?v1."+
                "}";

        QC.setFactory(dataset.getContext(), new OpExecutorSage.OpExecutorSageFactory(ARQ.getContext()));
        QueryEngineSage.register();

        SageInput<?> input = new SageInput<>();
        Context c = dataset.getContext().copy().set(SageConstants.input, input);
        c.set(ARQ.optimization, false);

        QueryExecution queryExecution = null;
        try {
            queryExecution = QueryExecution.create()
                    .dataset(dataset)
                    .context(c)
                    .query(query).build();
        } catch (Exception e) {
            e.printStackTrace();
        }

        long nbResults = 0;
        ResultSet rs = queryExecution.execSelect() ;
        while (rs.hasNext()) {
            rs.next();
            nbResults+=1;
        }

    }


    @Disabled
    @Test
    public void execute_vpo_with_TDB() {
        String query = "SELECT * WHERE {" +
                "?v0 <http://db.uwaterloo.ca/~galuc/wsdbm/gender> <http://db.uwaterloo.ca/~galuc/wsdbm/Gender0>"+
                "}";

        Optional dirPath_opt = Optional.of("target");
        Watdiv10M watdiv = new Watdiv10M(dirPath_opt); // creates the db if need be

        Dataset dataset = TDB2Factory.connectDataset(watdiv.dbPath_asStr);
        dataset.begin(ReadWrite.READ);

        QC.setFactory(dataset.getContext(), OpExecutorTDB2.OpExecFactoryTDB);
        QueryEngineTDB.register();

        Context c = dataset.getContext().copy();
        c.set(ARQ.optimization, false);

        QueryExecution queryExecution = null;
        try {
            queryExecution = QueryExecution.create()
                    .dataset(dataset)
                    .context(c)
                    .query(query).build();
        } catch (Exception e) {
            e.printStackTrace();
        }

        long nbResults = 0;
        ResultSet rs = queryExecution.execSelect() ;
        while (rs.hasNext()) {
            rs.next();
            nbResults+=1;
        }


        dataset.end();
    }

}