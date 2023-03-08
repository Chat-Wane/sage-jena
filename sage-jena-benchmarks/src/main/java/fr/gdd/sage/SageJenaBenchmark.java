package fr.gdd.sage;


import fr.gdd.sage.arq.OpExecutorSage;
import fr.gdd.sage.arq.QueryEngineSage;
import fr.gdd.sage.arq.SageConstants;
import fr.gdd.sage.io.SageInput;
import fr.gdd.sage.io.SageOutput;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.jena.query.*;
import org.apache.jena.sparql.engine.Plan;
import org.apache.jena.sparql.engine.QueryEngineRegistry;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.BindingRoot;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.tdb2.TDB2Factory;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;
import org.slf4j.simple.SimpleLogger;
import org.slf4j.simple.SimpleLoggerConfiguration;

import java.io.*;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;


public class SageJenaBenchmark {
    static Logger log = LoggerFactory.getLogger(SageJenaBenchmark.class);

    static Path dbPath;

    @State(Scope.Benchmark)
    public static class Backend {
        volatile Dataset dataset;

        @Setup
        public void prepare() {
            // (TODO) change dbPath so its the one read by
            dbPath = Paths.get("target", "watdiv10M");
            dataset = TDB2Factory.connectDataset(dbPath.toString());
            dataset.begin(ReadWrite.READ);

            // kept outside of @Benchmark
            QC.setFactory(dataset.getContext(), new OpExecutorSage.OpExecutorSageFactory(ARQ.getContext()));
            QueryEngineRegistry.addFactory(QueryEngineSage.factory);
        }

        @TearDown
        public void close() {
            dataset.end();
        }
    }

    @Benchmark
    public long query(Backend b) {
        String query_as_str = "SELECT ?o WHERE {<http://db.uwaterloo.ca/~galuc/wsdbm/Retailer6> ?p ?o .}";
        Query query = QueryFactory.create(query_as_str);

        SageInput<?> input = new SageInput<>();
        Context c = b.dataset.getContext().copy().set(SageConstants.input, input);
        Plan plan = QueryEngineSage.factory.create(query, b.dataset.asDatasetGraph(), BindingRoot.create(), c);
        QueryIterator it = plan.iterator();

        long nb_results = 0;
        while (it.hasNext()) {
            it.next();
            nb_results += 1;
        }

        it.close();

        SageOutput<?> output = c.get(SageConstants.output);

        return output.size();
    }

    /**
     * Run the benchmark on Watdiv.
     * @param args [0] The path to the DB directory (default: "target").
     * @throws RunnerException
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
            List<String> whiteList = Collections.singletonList("watdiv.10M.nt");
            try {
                FileInputStream in = new FileInputStream(filePath.toString());
                InputStream bin = new BufferedInputStream(in);
                BZip2CompressorInputStream bzIn = new BZip2CompressorInputStream(bin);
                TarArchiveInputStream tarIn = new TarArchiveInputStream(bzIn);

                ArchiveEntry entry = null;

                if (!Files.exists(extractPath)) {
                    Files.createDirectory(extractPath);
                }
                while (!Objects.isNull(entry = tarIn.getNextEntry())) {
                    if (entry.getSize() < 1) {
                        continue;
                    }
                    Path entryExtractPath = Paths.get(extractPath.toString(), entry.getName());
                    if (Files.exists(entryExtractPath) || !whiteList.contains(entry.getName())) {
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
            Path entryExtractPath = Paths.get(extractPath.toString(), whiteList.get(0));
            // (TODO) default or union ?
            dataset.getDefaultModel().read(entryExtractPath.toString());
            dataset.commit();
            dataset.end();
            log.info("Done with the database.");
        }

        Options opt = new OptionsBuilder()
                .include(SageJenaBenchmark.class.getSimpleName())
                .forks(1)
                .threads(1)
                .build();

        new Runner(opt).run();
    }

}
