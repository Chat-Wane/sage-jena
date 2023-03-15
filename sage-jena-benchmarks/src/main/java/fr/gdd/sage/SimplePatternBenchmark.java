package fr.gdd.sage;

import fr.gdd.sage.arq.QueryEngineSage;
import fr.gdd.sage.arq.SageConstants;
import fr.gdd.sage.datasets.Watdiv10M;
import fr.gdd.sage.io.SageInput;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.sparql.engine.main.OpExecutorFactory;
import org.apache.jena.sparql.util.Context;
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
@BenchmarkMode({Mode.SingleShotTime})
@State(Scope.Benchmark)
@Warmup(time = 5, iterations = 5)
@Measurement(iterations = 1)
public class SimplePatternBenchmark {
    static Logger log = LoggerFactory.getLogger(SimplePatternBenchmark.class);


    static OpExecutorFactory opExecutorTDB2ForceOrderFactory;

    @Param({"tdb", "sage", "tdb force order", "sage force order"})
    public String b_engine;

    @Param("target/watdiv10M")
    public String z_dbPath;

    @Param({
            "<http://db.uwaterloo.ca/~galuc/wsdbm/City193> <http://www.geonames.org/ontology#parentCountry> ?v1." +
                    "?v6 <http://schema.org/nationality> ?v1." +
                    "?v6 <http://db.uwaterloo.ca/~galuc/wsdbm/likes> ?v3." +
                    "?v2 <http://purl.org/goodrelations/includes> ?v3." +
                    "?v2 <http://purl.org/goodrelations/validThrough> ?v5." +
                    "?v2 <http://purl.org/goodrelations/serialNumber> ?v4." +
                    "?v2 <http://schema.org/eligibleQuantity> ?v8." +
                    "?v6 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?v7." +
                    "?v9 <http://db.uwaterloo.ca/~galuc/wsdbm/purchaseFor> ?v3." +
                    "?v2 <http://schema.org/eligibleRegion> ?v1.",
            "?v0 <http://db.uwaterloo.ca/~galuc/wsdbm/gender> <http://db.uwaterloo.ca/~galuc/wsdbm/Gender0>", // vPO
            "?v0 <http://xmlns.com/foaf/familyName> ?v1." // vPv
    })
    public String a_pattern; // prefixed with alphanumeric character to force the execution order of @Param

    @Setup(Level.Trial)
    public void setup(SetupBenchmark.ExecutionContext ec) {
        SetupBenchmark.setup(ec, z_dbPath, b_engine);
    }

    @TearDown(Level.Trial)
    public void setdown(SetupBenchmark.ExecutionContext ec) {
        SetupBenchmark.setdown(ec, b_engine);
    }

    @Setup(Level.Trial)
    public void read_query(SetupBenchmark.ExecutionContext ec) {
        ec.query = "SELECT * WHERE {" + a_pattern + "}";
        log.debug("{}", ec.query);
    }

    @Setup(Level.Invocation)
    public void create_query_execution_plan(SetupBenchmark.ExecutionContext ec) {
        SageInput<?> input = new SageInput<>();
        Context c = ec.dataset.getContext().copy().set(SageConstants.input, input);

        try {
            ec.queryExecution = QueryExecution.create()
                    .dataset(ec.dataset)
                    .context(c)
                    .query(ec.query).build();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Benchmark
    public long execute_pattern(SetupBenchmark.ExecutionContext ec) {
        long nbResults = 0;
        ResultSet rs = ec.queryExecution.execSelect() ;
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
