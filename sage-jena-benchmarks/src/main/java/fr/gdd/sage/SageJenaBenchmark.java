package fr.gdd.sage;


import fr.gdd.sage.arq.OpExecutorSage;
import fr.gdd.sage.arq.QueryEngineSage;
import fr.gdd.sage.arq.SageConstants;
import fr.gdd.sage.generics.Pair;
import fr.gdd.sage.io.SageInput;
import fr.gdd.sage.io.SageOutput;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.jena.query.*;
import org.apache.jena.sparql.engine.Plan;
import org.apache.jena.sparql.engine.QueryEngineFactory;
import org.apache.jena.sparql.engine.QueryEngineRegistry;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.BindingRoot;
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

import javax.swing.filechooser.FileNameExtensionFilter;
import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;

@BenchmarkMode({Mode.SingleShotTime})
@Warmup(iterations = 3)
@State(Scope.Benchmark)
public class SageJenaBenchmark {
    static Logger log = LoggerFactory.getLogger(SageJenaBenchmark.class);

    static Path dbPath;

    static HashMap<String, Long> nbResultsPerQuery = new HashMap<>();

    @Param("SELECT * WHERE {?s ?p ?o}")
    public String query;

    @Param({"default", "sage"})
    public String engine;


    @State(Scope.Benchmark)
    public static class Backend {
        volatile Dataset dataset;

        @Setup
        public void open() {
            // (TODO) change dbPath so its the one read by
            dbPath = Paths.get("target", "watdiv10M");
            dataset = TDB2Factory.connectDataset(dbPath.toString());
            dataset.begin(ReadWrite.READ);
        }

        @TearDown
        public void close() {
            dataset.end();
        }
    }

    @Setup
    public void setup_engine(Backend b) {
        if (engine.equals("default")) {
            QC.setFactory(b.dataset.getContext(), OpExecutorTDB2.OpExecFactoryTDB);
            QueryEngineTDB.register();
        } else {
            QC.setFactory(b.dataset.getContext(), new OpExecutorSage.OpExecutorSageFactory(ARQ.getContext()));
            QueryEngineRegistry.addFactory(QueryEngineSage.factory);
        }
    }

    @TearDown
    public void setdown_engine() {
        if (engine.equals("default")) {
            QueryEngineTDB.unregister();
        } else {
            QueryEngineRegistry.removeFactory(QueryEngineSage.factory); // (TODO) register unregister
        }
    }

    @Benchmark
    public long execute_query(Backend b) {
        Query q = QueryFactory.create(query);

        SageInput<?> input = new SageInput<>();
        Context c = b.dataset.getContext().copy().set(SageConstants.input, input);
        QueryEngineFactory factory = QueryEngineRegistry.findFactory(q, b.dataset.asDatasetGraph(), c);
        Plan plan = factory.create(q, b.dataset.asDatasetGraph(), BindingRoot.create(), c);
        QueryIterator it = plan.iterator();

        long nbResults = 0;
        while (it.hasNext()) {
            it.next();
            nbResults += 1;
        }
        it.close();

        // SageOutput<?> output = c.get(SageConstants.output);
        // return output.size();

        // (TODO) remove this from the benchmarked part
        if (nbResultsPerQuery.containsKey(query)) {
            if (nbResultsPerQuery.get(query) != nbResults) {
                System.out.println("/!\\ not the same number of results");
            }
        } else {
            nbResultsPerQuery.put(query, nbResults);
        }

        return nbResults;
    }

    /**
     * Run the benchmark on Watdiv.
     * @param args [0] The path to the DB directory (default: "target").
     */
    public static void main(String[] args) throws RunnerException {

        Path dirPath = (args.length > 0) ? Paths.get(args[0]) : Paths.get("target");
        dbPath = Paths.get(dirPath.toString(), "watdiv10M");
        Path filePath = Paths.get(dirPath.toString(), "watdiv.10M.tar.bz2");
        Path extractPath = Paths.get(dirPath.toString(), "watdiv.10M");

        System.out.printf("ARGS : %s\n", Arrays.toString(args));

        // (TODO) refactor so downloading, extract, creating db is in its own class.
        if (Files.exists(dbPath)) {
            log.info("Database already exists, starting benchmark…");
        } else {
            log.info("Database does not exist, creating it");
            if (!Files.exists(filePath)) {
                log.info("Starting the download…");
                // (TODO) create a logger to inform about process
                // #1 download the file from the internet
                String watdivUrl = "https://dsg.uwaterloo.ca/watdiv/watdiv.10M.tar.bz2";

                try (BufferedInputStream in = new BufferedInputStream(new URL(watdivUrl).openStream());
                     FileOutputStream fileOutputStream = new FileOutputStream(filePath.toString())) {
                    byte dataBuffer[] = new byte[1024];
                    int bytesRead;
                    while ((bytesRead = in.read(dataBuffer, 0, 1024)) != -1) {
                        fileOutputStream.write(dataBuffer, 0, bytesRead);
                    }
                    fileOutputStream.flush();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            // #2 unzip it if need be
            List<String> whitelist = Collections.singletonList("watdiv.10M.nt");
            try {
                FileInputStream in = new FileInputStream(filePath.toString());
                InputStream bin = new BufferedInputStream(in);
                BZip2CompressorInputStream bzIn = new BZip2CompressorInputStream(bin);
                TarArchiveInputStream tarIn = new TarArchiveInputStream(bzIn);

                ArchiveEntry entry;

                if (!Files.exists(extractPath)) {
                    Files.createDirectory(extractPath);
                }
                while (!Objects.isNull(entry = tarIn.getNextEntry())) {
                    if (entry.getSize() < 1) {
                        continue;
                    }
                    Path entryExtractPath = Paths.get(extractPath.toString(), entry.getName());
                    if (Files.exists(entryExtractPath) || !whitelist.contains(entry.getName())) {
                        log.info("Skipping file {}…", entryExtractPath);
                        // still slows… for it must read the bytes to skip them
                        tarIn.skip(entry.getSize());
                        continue;
                    }
                    log.info("Extracting file {}…", entryExtractPath);
                    entryExtractPath.toFile().createNewFile();
                    try (FileOutputStream writer = new FileOutputStream(entryExtractPath.toFile())) {
                        byte dataBuffer[] = new byte[1024];
                        int bytesRead;
                        while ((bytesRead = tarIn.read(dataBuffer, 0, 1024)) != -1) {
                            writer.write(dataBuffer, 0, bytesRead);
                        }
                        writer.flush();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                tarIn.close();
            } catch (Exception e){
                e.printStackTrace();
            }

            // #3 ingest in jena database
            log.info("Starting to create the actual Jena TDB2 database…");
            Dataset dataset = TDB2Factory.connectDataset(dbPath.toString());
            dataset.begin(ReadWrite.WRITE);
            // (TODO) all whitelisted
            Path entryExtractPath = Paths.get(extractPath.toString(), whitelist.get(0));
            // (TODO) default or union ?
            dataset.getDefaultModel().read(entryExtractPath.toString());
            dataset.commit();
            dataset.end();
            log.info("Done with the database.");
        }

        Path queriesPath = Paths.get("sage-jena-benchmarks" , "queries", "watdiv_with_sage_plan");
        File[] queryFiles = queriesPath.toFile().listFiles((dir, name) -> name.endsWith(".sparql"));
        ArrayList<String> queries = new ArrayList<>();
        List<String> blacklist = List.of();
        log.info("Queries folder contains {} SPARQL queries.", queryFiles.length);
        for (File queryFile : queryFiles) {
            if (blacklist.contains(queryFile.getName())) { continue; }
            if (queries.size() > 0) { break; } // (for testing purpose)
            try {
                String query = Files.readString(queryFile.toPath(), StandardCharsets.UTF_8);
                query = query.replace('\n', ' '); // to get a clearer one line rendering
                query = query.replace('\t', ' ');
                query = String.format("# %s\n%s", queryFile.getName(), query);
                queries.add(query);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        String[] queriesAsArray = queries.toArray(String[]::new);

        Options opt = new OptionsBuilder()
                .include(".*" + SageJenaBenchmark.class.getSimpleName() + ".*")
                .param("query", queriesAsArray)
                .forks(1)
                .threads(1) // (TODO) manage to up this number, for now, `Maximum lock count exceeded`… or other
                .build();

        new Runner(opt).run();
    }

}
