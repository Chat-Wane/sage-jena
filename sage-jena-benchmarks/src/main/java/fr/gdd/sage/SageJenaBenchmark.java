package fr.gdd.sage;


import fr.gdd.sage.jena.JenaBackend;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.tdb2.TDB2Factory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        volatile Dataset dataset = TDB2Factory.connectDataset(dbPath.toString());
        
    }

    @Benchmark
    public boolean query(Backend b) {
        String query_as_str = "SELECT ?o WHERE {<http://db.uwaterloo.ca/~galuc/wsdbm/Retailer6> ?p ?o .}";
        Query query = QueryFactory.create(query_as_str);

        return true;
    }

    public static void main(String[] args) throws RunnerException {

        Path dirPath = (args.length > 0) ? Paths.get(args[0]) : Paths.get("target");
        dbPath = Paths.get(dirPath.toString(), "watdiv10M");
        Path filePath = Paths.get(dirPath.toString(), "watdiv.10M.tar.bz2");
        Path extractPath = Paths.get(dirPath.toString(), "watdiv.10M");

        System.out.printf("ARGS : %s\n", Arrays.toString(args));

        // (TODO) refactor so downloading, extract, creating db is in its own class.
        if (!Files.exists(dbPath)) {
            log.info("The watdiv database does not exist, creating it");
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
                .threads(1)
                .build();

        new Runner(opt).run();
    }

}
