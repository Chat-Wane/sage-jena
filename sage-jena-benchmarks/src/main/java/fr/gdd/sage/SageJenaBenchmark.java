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
import org.apache.jena.tdb2.store.DatasetGraphTDB;
import org.apache.jena.tdb2.sys.TDBInternal;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Optional;

@BenchmarkMode({Mode.SingleShotTime})
@Warmup(iterations = 5)
@State(Scope.Benchmark)
public class SageJenaBenchmark {
    static Logger log = LoggerFactory.getLogger(SageJenaBenchmark.class);

    static Path dbPath;

    static HashMap<String, Long> nbResultsPerQuery = new HashMap<>();

    @Param("SELECT * WHERE {?s ?p ?o}")
    public String a_query;

    @Param({"default", "sage"})
    public String b_engine;


    @State(Scope.Thread)
    public static class Backend {
        volatile Dataset dataset;

        @Setup
        public void open() {
            // (TODO) change dbPath so its the one read by
            dbPath = Paths.get("target", "watdiv10M");
            dataset = TDB2Factory.connectDataset(dbPath.toString());
            DatasetGraphTDB graph = TDBInternal.getDatasetGraphTDB(this.dataset);
            if (!dataset.isInTransaction()) {
                dataset.begin(ReadWrite.READ);
            }
        }

        @TearDown
        public void close() {
            if (dataset.isInTransaction()) {
                dataset.end();
            }
        }
    }

    @Setup
    public void setup_engine(Backend b) {
        if (b_engine.equals("default")) {
            QC.setFactory(b.dataset.getContext(), OpExecutorTDB2.OpExecFactoryTDB);
            QueryEngineTDB.register();
        } else {
            QC.setFactory(b.dataset.getContext(), new OpExecutorSage.OpExecutorSageFactory(ARQ.getContext()));
            QueryEngineSage.register();
        }
    }

    @TearDown
    public void setdown_engine() {
        if (b_engine.equals("default")) {
            QueryEngineTDB.unregister();
        } else {
            QueryEngineSage.unregister();
        }
    }

    @Benchmark
    public long execute_query(Backend b) {
        //Query q = QueryFactory.create(query);

        // (TODO) maybe build plan outside of execute query ?
        SageInput<?> input = new SageInput<>();
        Context c = b.dataset.getContext().copy().set(SageConstants.input, input);
        c.set(ARQ.optimization, false);

/*        QueryEngineFactory factory = QueryEngineRegistry.findFactory(q, b.dataset.asDatasetGraph(), c);
        Plan plan = factory.create(q, b.dataset.asDatasetGraph(), BindingRoot.create(), c);*/

        long nbResults = 0;
        try(QueryExecution qExec = QueryExecution.create()
                .dataset(b.dataset)
                .context(c)
                .query(a_query).build()) { // .set(ARQ.symLogExec, Explain.InfoLevel.ALL).build() ) {
            ResultSet rs = qExec.execSelect() ;
            while (rs.hasNext()) {
                rs.next();
                nbResults+=1;
            }
        }

/*        QueryIterator it = plan.iterator();

        long nbResults = 0;
        while (it.hasNext()) {
            it.next();
            nbResults += 1;
        }
        it.close();*/

        // SageOutput<?> output = c.get(SageConstants.output);
        // return output.size();

        // (TODO) remove this from the benchmarked part
        if (nbResultsPerQuery.containsKey(a_query)) {
            if (nbResultsPerQuery.get(a_query) != nbResults) {
                System.out.println("/!\\ not the same number of results");
            }
        } else {
            nbResultsPerQuery.put(a_query, nbResults);
        }

        return nbResults;
    }

    /**
     * Run the benchmark on Watdiv.
     * @param args [0] The path to the DB directory (default: "target").
     */
    public static void main(String[] args) throws RunnerException {
        Optional<String> dirPath_opt = (args.length > 0) ? Optional.of(args[0]) : Optional.empty();

        Watdiv10M watdiv = new Watdiv10M(dirPath_opt); // creates the db if need be

        String[] queriesAsArray = watdiv.queries.stream().map(p -> p.left).toArray(String[]::new);

        Options opt = new OptionsBuilder()
                .include(".*" + SageJenaBenchmark.class.getSimpleName() + ".*")
                .param("a_query", queriesAsArray)
                .forks(1)
                .threads(1)
                .build();

        new Runner(opt).run();
    }

}
