package fr.gdd.sage;



import java.util.Map;

import org.apache.jena.query.ARQ;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.engine.main.StageBuilder;
import org.apache.jena.sparql.engine.main.StageGenerator;
import org.apache.jena.tdb2.TDB2Factory;
import org.apache.jena.tdb2.store.DatasetGraphTDB;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import fr.gdd.sage.arq.SageConstants;
import fr.gdd.sage.arq.SageOpExecutorFactory;
import fr.gdd.sage.arq.SageStageGenerator;
import fr.gdd.sage.io.SageInput;
import fr.gdd.sage.io.SageOutput;
import fr.gdd.sage.jena.JenaBackend;
import fr.gdd.sage.jena.SerializableRecord;



public class SageJenaBenchmark {

    @State(Scope.Benchmark)
    public static class Backend {
        String path = "/Users/nedelec-b-2/Desktop/Projects/sage-jena/sage-jena-module/watdiv10M";
        volatile Dataset dataset = TDB2Factory.connectDataset(path);
        volatile JenaBackend backend = new JenaBackend(path);
    }

    @Benchmark
    public boolean query(Backend b) {
        String query_as_str = "SELECT ?o WHERE {<http://db.uwaterloo.ca/~galuc/wsdbm/Retailer6> ?p ?o .}";
        Query query = QueryFactory.create(query_as_str);

        return true;
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
            .include(Backend.class.getSimpleName())
            .threads(4)
            .forks(1)
            .build();
        
        new Runner(opt).run();
    }

}
