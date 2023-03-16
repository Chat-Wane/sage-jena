package fr.gdd.sage;


import fr.gdd.sage.datasets.Watdiv10M;
import fr.gdd.sage.generics.Pair;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Optional;

// (TODO) different Mode depending on the time spent by queries. Need to run them once
// at least though.
@BenchmarkMode({Mode.SingleShotTime})
@Warmup(iterations = 3)
@State(Scope.Benchmark)
public class WatdivBenchmark {
    static Logger log = LoggerFactory.getLogger(WatdivBenchmark.class);

    static HashMap<String, Long> nbResultsPerQuery = new HashMap<>();

    @Param("sage-jena-benchmarks/queries/watdiv_with_sage_plan/query_10084.sparql")
    public String a_query;

    @Param({EngineTypes.TDB, EngineTypes.Sage})
            // EngineTypes.TDBForceOrder, EngineTypes.SageForceOrder })
           // EngineTypes.SageForceOrderTimeout60s, EngineTypes.SageForceOrderTimeout30s,
           // EngineTypes.SageForceOrderTimeout10s, EngineTypes.SageForceOrderTimeout1s})
    public String b_engine;

    @Param("target/watdiv10M")
    public String z_dbPath;


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
        try {
            ec.query = Files.readString(Paths.get(a_query), StandardCharsets.UTF_8);
        } catch (Exception e) {
            e.printStackTrace();
        }
        log.debug("{}", ec.query);
    }

    @Benchmark
    public long execute(SetupBenchmark.ExecutionContext ec) throws Exception {
        Pair<Long, Long> nbResultsAndPreempt = SetupBenchmark.execute(ec, b_engine);

        // (TODO) remove this from the benchmarked part
        if (nbResultsPerQuery.containsKey(a_query)) {
            long previousNbResults = nbResultsPerQuery.get(a_query);
            if (previousNbResults != nbResultsAndPreempt.left) {
                throw (new Exception(String.format("/!\\ not the same number of results on %s: %s vs %s.",
                        a_query, previousNbResults, nbResultsAndPreempt.left)));
            }
        } else {
            nbResultsPerQuery.put(a_query, nbResultsAndPreempt.left);
        }

        log.debug("Got {} results for this query.", nbResultsAndPreempt.left);

        return nbResultsAndPreempt.left;
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
                .include(".*" + WatdivBenchmark.class.getSimpleName() + ".*")
                .param("a_query", queriesAsArray)
                .param("z_dbPath", watdiv.dbPath_asStr)
                .forks(1)
                .threads(1)
                .result("sage-jena-benchmarks/results/WatdivBenchmarkBase.csv")
                .resultFormat(ResultFormatType.CSV)
                .build();

        new Runner(opt).run();
    }

}
