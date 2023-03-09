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
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

/**
 * Queries from benchmarks usually comprise specific SPARQL operations. For
 * instance, `watdiv` only has joins over triples; `jobrdf` has joins, and
 * filters.
 *
 * This benchmark aims to evaluate the cost of simple single triple patterns,
 * once again with preemptive volcano against simple volcano.
 */
@State(Scope.Benchmark)
@Warmup(time = 5, iterations = 3)
@Measurement(iterations = 1)
public class SimplePatternBenchmark {

    @Param({
            "?v0 <http://db.uwaterloo.ca/~galuc/wsdbm/gender> <http://db.uwaterloo.ca/~galuc/wsdbm/Gender0>", // vPO
            "?v0 <http://xmlns.com/foaf/familyName> ?v1." // vPv
    })
    public String a_pattern; // prefixed with alphanumeric character to force the execution order of @Param

    @Param({"default", "sage"})
    public String b_engine;
    

    @State(Scope.Benchmark)
    public static class Backend {
        volatile Dataset dataset;

        @Setup
        public void open() {
            // (TODO) change dbPath so its the one read by
            Path dbPath = Paths.get("target", "watdiv10M");
            dataset = TDB2Factory.connectDataset(dbPath.toString());

        }

        @TearDown
        public void close() {

        }
    }

    @Setup
    public void setupEngine(Backend b) {
        b.dataset.begin(ReadWrite.READ);
        if (b_engine.equals("default")) {
            QC.setFactory(b.dataset.getContext(), OpExecutorTDB2.OpExecFactoryTDB);
            QueryEngineTDB.register();
        } else {
            QC.setFactory(b.dataset.getContext(), new OpExecutorSage.OpExecutorSageFactory(ARQ.getContext()));
            QueryEngineSage.register();
        }
    }

    @TearDown
    public void setdownEngine(Backend b) {
        if (b_engine.equals("default")) {
            QueryEngineTDB.unregister();
        } else {
            QueryEngineSage.unregister();
        }
        b.dataset.end();
    }

    @Benchmark
    public long execute_pattern(Backend b) {
        SageInput<?> input = new SageInput<>();
        Context c = b.dataset.getContext().copy().set(SageConstants.input, input);
        c.set(ARQ.optimization, false);

        String query = "SELECT * WHERE {"+ a_pattern + "}";
        long nbResults = 0;
        try(QueryExecution qExec = QueryExecution.create()
                .dataset(b.dataset)
                .context(c)
                .query(query).build()) { // .set(ARQ.symLogExec, Explain.InfoLevel.ALL).build() ) {
            ResultSet rs = qExec.execSelect() ;
            while (rs.hasNext()) {
                rs.next();
                nbResults += 1;
            }
        }
        return nbResults;
    }

    public static void main(String[] args) throws RunnerException {
        Optional<String> dirPath_opt = (args.length > 0) ? Optional.of(args[0]) : Optional.empty();

        new Watdiv10M(dirPath_opt); // creates the db if need be

        Options opt = new OptionsBuilder()
                .include(SimplePatternBenchmark.class.getSimpleName())
                .forks(1)
                .threads(1) // (TODO) manage to up this number, for now, `Maximum lock count exceeded`… or other
                .build();

        new Runner(opt).run();

    }
}
