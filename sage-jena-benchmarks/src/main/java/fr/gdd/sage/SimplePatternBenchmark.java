package fr.gdd.sage;

import fr.gdd.sage.arq.OpExecutorSage;
import fr.gdd.sage.arq.QueryEngineSage;
import fr.gdd.sage.arq.SageConstants;
import fr.gdd.sage.datasets.Watdiv10M;
import fr.gdd.sage.io.SageInput;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.ResultSet;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
@Warmup(time = 5, iterations = 5)
@Measurement(iterations = 1)
public class SimplePatternBenchmark {
    static Logger log = LoggerFactory.getLogger(SimplePatternBenchmark.class);

    @Param({
            "?v0 <http://db.uwaterloo.ca/~galuc/wsdbm/gender> <http://db.uwaterloo.ca/~galuc/wsdbm/Gender0>", // vPO
            "?v0 <http://xmlns.com/foaf/familyName> ?v1." // vPv
    })
    public String a_pattern; // prefixed with alphanumeric character to force the execution order of @Param

    @Param({"default", "sage"})
    public String b_engine;

    @Param("target/watdiv10M")
    public String z_dbPath;


    @Setup(Level.Trial)
    public void setupEngine(WatdivBenchmark.Backend b) {
        b.dataset = TDB2Factory.connectDataset(z_dbPath);
        if (!b.dataset.isInTransaction()) {
            b.dataset.begin(ReadWrite.READ);
        }

        if (b_engine.equals("default")) {
            QC.setFactory(b.dataset.getContext(), OpExecutorTDB2.OpExecFactoryTDB);
            QueryEngineTDB.register();
        } else {
            QC.setFactory(b.dataset.getContext(), new OpExecutorSage.OpExecutorSageFactory(ARQ.getContext()));
            QueryEngineSage.register();
        }
    }

    @TearDown(Level.Trial)
    public void setdownEngine(WatdivBenchmark.Backend b) {
        if (b_engine.equals("default")) {
            QueryEngineTDB.unregister();
        } else {
            QueryEngineSage.unregister();
        }
        if (b.dataset.isInTransaction()) {
            b.dataset.end();
        }
    }

    @Setup(Level.Trial)
    public void read_query(WatdivBenchmark.Backend b) {
        b.query = "SELECT * WHERE {" + a_pattern + "}";
        log.debug("{}", b.query);
    }

    @Setup(Level.Invocation)
    public void create_query_execution_plan(WatdivBenchmark.Backend b) {
        SageInput<?> input = new SageInput<>();
        Context c = b.dataset.getContext().copy().set(SageConstants.input, input);
        c.set(ARQ.optimization, false);

        try {
            b.queryExecution = QueryExecution.create()
                    .dataset(b.dataset)
                    .context(c)
                    .query(b.query).build();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Benchmark
    public long execute_pattern(WatdivBenchmark.Backend b) {
        long nbResults = 0;
        ResultSet rs = b.queryExecution.execSelect() ;
        while (rs.hasNext()) {
            rs.next();
            nbResults+=1;
        }

        log.debug("Got {} results for this query.", nbResults);

        return nbResults;
    }

    public static void main(String[] args) throws RunnerException {
        Optional<String> dirPath_opt = (args.length > 0) ? Optional.of(args[0]) : Optional.empty();

        Watdiv10M watdiv = new Watdiv10M(dirPath_opt); // creates the db if need be

        Options opt = new OptionsBuilder()
                .include(SimplePatternBenchmark.class.getSimpleName())
                .param("z_dbPath", watdiv.dbPath_asStr)
                .forks(1)
                .threads(1) // (TODO) manage to up this number, for now, `Maximum lock count exceeded`… or other
                .build();

        new Runner(opt).run();

    }
}
