package fr.gdd.sage;

import fr.gdd.sage.generics.Pair;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Dataset;
import org.apache.jena.sparql.engine.main.OpExecutorFactory;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.tdb2.solver.OpExecutorTDB2;
import org.apache.jena.tdb2.solver.QueryEngineTDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;

/**
 * Execute and register a benchmark (dataset along with queries) using Jena with force order. Measurements
 * are saved in a `.csv` file. It contains the query name, the execution time, and the number of results.
 */
public class Baseline {

    Logger log = LoggerFactory.getLogger(Baseline.class);

    final Dataset dataset;
    List<String> queries;
    String output;
    Integer nbTimes;

    String[] HEADERS = { "query", "results", "execution time (ms)"};

    HashMap<String, Long> query2Results = new HashMap<>();
    HashMap<String, Long> query2Time    = new HashMap<>();

    public Baseline(Dataset dataset, List<String> queries,  Integer nbTimes, String output) {
        this.dataset = dataset;
        this.queries = queries;
        this.output = output;
        this.nbTimes = nbTimes;
    }

    public void execute() throws IOException {
        Path output_asPath = Path.of(output);

        if (output_asPath.toFile().exists()) {
            log.info("Reading the already existing output file.");
            CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                    .setHeader(HEADERS)
                    .setSkipHeaderRecord(true)
                    .build();

            Reader in = new FileReader(output_asPath.toString());

            Iterable<CSVRecord> records = csvFormat.parse(in);

            for (CSVRecord record : records) {
                String query = record.get(HEADERS[0]);
                Long results = Long.parseLong(record.get(HEADERS[1]));
                Long time = Long.parseLong(record.get(HEADERS[2]));
                query2Results.put(query, results);
                query2Time.put(query, time);
                log.info("Skipping {}…", query);
            }
        } else {
            log.info("Creating a fresh output file at {}.", output_asPath.toAbsolutePath());
            output_asPath.toFile().createNewFile();
        }

        Field plainFactoryField = ReflectionUtils._getField(OpExecutorTDB2.class, "plainFactory");
        OpExecutorFactory opExecutorTDB2ForceOrderFactory = (OpExecutorFactory) ReflectionUtils._callField(plainFactoryField, OpExecutorTDB2.class, null);
        dataset.getContext().set(ARQ.optimization, false);
        QC.setFactory(dataset.getContext(), opExecutorTDB2ForceOrderFactory);
        QueryEngineTDB.register();

        for (String query: queries) {
            if (query2Results.containsKey(query)) {
                continue;
            }

            log.info("Executing {}…", query);
            for (int i = 0; i < nbTimes; ++i) {
                long starting = System.currentTimeMillis();
                Pair<Long, Long> resultsAndPreempt = ExecuteUtils.executeTDB(dataset, query);
                long elapsed = System.currentTimeMillis() - starting;

                query2Results.put(query, resultsAndPreempt.left);
                query2Time.put(query, elapsed);

                log.debug("[{}], Got {} results in {} ms.", i, resultsAndPreempt.left, elapsed);

                CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                        .setHeader(HEADERS)
                        .setSkipHeaderRecord(true)
                        .build();

                csvFormat.printRecord(new FileWriter(output_asPath.toFile()), query, resultsAndPreempt.left, elapsed);
            }
        }
    }

}
