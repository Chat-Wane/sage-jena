package fr.gdd.sage;



import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.tdb2.TDB2Factory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import fr.gdd.sage.jena.JenaBackend;

import java.io.BufferedInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.Optional;


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
        // (TODO) find out if args are well read.
        // (TODO) use a CLI if need be
        Optional<String> filePath = (args.length > 0) ? Optional.of(args[0]) : Optional.empty();

        if (filePath.isPresent()) {
            // (TODO) create a logger to inform about process
            // #1 download the file from the internet
            String watdivUrl = "https://dsg.uwaterloo.ca/watdiv/watdiv.10M.tar.bz2";

            try (BufferedInputStream in = new BufferedInputStream(new URL(watdivUrl).openStream());
                 FileOutputStream fileOutputStream = new FileOutputStream(filePath.get())) {
                byte dataBuffer[] = new byte[1024];
                int bytesRead;
                while ((bytesRead = in.read(dataBuffer, 0, 1024)) != -1) {
                    fileOutputStream.write(dataBuffer, 0, bytesRead);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            // #2 unzip it if need be
            // #3 ingest in jena database
        }

        Options opt = new OptionsBuilder()
            .include(Backend.class.getSimpleName())
            .threads(4)
            .forks(1)
            .build();
        
        new Runner(opt).run();
    }

}
