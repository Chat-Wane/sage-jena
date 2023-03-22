package fr.gdd.sage.datasets;

import fr.gdd.sage.generics.Pair;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.tdb2.TDB2Factory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Open `watdiv` dataset or create (ie. download, unarchive, then ingest) it when
 * need be.
 *
 * (TODO) abstract this class so others can be built on the same pattern.
 */
public class Watdiv10M {
    static Logger log = LoggerFactory.getLogger(Watdiv10M.class);

    public static final String ARCHIVE_NAME = "watdiv.10M.tar.bz2";
    public static final String DB_NAME = "watdiv10M";
    public static final String EXTRACT_PATH = "extracted_watdiv.10M";
    public static final String DEFAULT_DB_PATH = "target";
    public static final String DOWNLOAD_URL = "https://dsg.uwaterloo.ca/watdiv/watdiv.10M.tar.bz2";

    public static final String QUERIES_PATH = "sage-jena-benchmarks/queries/watdiv_with_sage_plan";

    public static final List<String> whitelist = List.of("watdiv.10M.nt");
    // fails or too time consuming because no limit
    public static final List<String> blacklist = List.of("query_10069.sparql", "query_10150.sparql", "query_10091.sparql");

    public final String dbPath_asStr;
    public final List<Pair<String, String>> queries;

    // above 100s
    final List<String> longQueryNames = List.of("query_10020.sparql", "query_10082.sparql", "query_10168.sparql",
            "query_10078.sparql", "query_10083.sparql");
    public List<String> longQueries = new ArrayList<>();
    // between 1s to 100s
    final List<String> mediumQueryNames = List.of("query_10122.sparql", "query_10012.sparql", "query_10061.sparql");
    public List<String> mediumQueries = new ArrayList<>();
    // the rest below 1s

    public List<String> shortQueries = new ArrayList<>();

    public Watdiv10M(Optional<String> dbPath_opt) {

        Path dirPath = dbPath_opt.map(Paths::get).orElseGet(() -> Paths.get(DEFAULT_DB_PATH));
        Path dbPath = Paths.get(dirPath.toString(), DB_NAME);
        Path filePath = Paths.get(dirPath.toString(), ARCHIVE_NAME);
        Path extractPath = Paths.get(dirPath.toString(), EXTRACT_PATH);

        if (Files.exists(dbPath)) {
            log.info("Database already exists, skipping creation.");
        } else {
            log.info("Database does not exist, creating it…");
            download(filePath, DOWNLOAD_URL);
            extract(filePath, extractPath, whitelist);
            ingest(dbPath, extractPath, whitelist);
            log.info("Done with the database {}.", dbPath);
        }

        this.dbPath_asStr = dbPath.toString();

        log.info("Reading queries…");
        this.queries = getQueries(QUERIES_PATH, blacklist);

        categorizeQueries(queries);
    }

    /**
     * Divide the performance analysis into 3 categories to ease benchmarking.
     */
    public void categorizeQueries(List<Pair<String, String>> queries) {
        longQueries = new ArrayList<>();
        mediumQueries = new ArrayList<>();
        shortQueries = new ArrayList<>();
        for (String queryName : queries.stream().map((p) -> p.left).toList()) {
            if (!longQueryNames.stream().filter(e -> queryName.contains(e)).toList().isEmpty()) {
                longQueries.add(queryName);
            } else if (!mediumQueryNames.stream().filter(e -> queryName.contains(e)).toList().isEmpty()) {
                mediumQueries.add(queryName);
            } else {
                shortQueries.add(queryName);
            }
        }
    }

    /**
     * @return A list of pairs containing the name of the query and its actual content.
     */
    static public ArrayList<Pair<String,String>> getQueries(String queriesPath_asStr, List<String> blacklist) {
        ArrayList<Pair<String, String>> queries = new ArrayList<>();
        Path queriesPath = Paths.get(queriesPath_asStr);
        if (!queriesPath.toFile().exists()) {
            return queries; // no queries
        };

        File[] queryFiles = queriesPath.toFile().listFiles((dir, name) -> name.endsWith(".sparql"));

        log.info("Queries folder contains {} SPARQL queries.", queryFiles.length);
        for (File queryFile : queryFiles) {
            if (blacklist.contains(queryFile.getName())) { continue; }
            try {
                String query = Files.readString(queryFile.toPath(), StandardCharsets.UTF_8);
                query = query.replace('\n', ' '); // to get a clearer one line rendering
                query = query.replace('\t', ' ');
                // query = String.format("# %s\n%s", queryFile.getName(), query);
                queries.add(new Pair<>(queryFile.getPath(), query));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return queries;
    }

    /**
     * Download the dataset from remote URL.
     * @param path The file location to download to.
     * @param url The URL of the content to download.
     */
    static public void download(Path path, String url) {
        if (!Files.exists(path)) {
            log.info("Starting the download…");

            try (BufferedInputStream in = new BufferedInputStream(new URL(url).openStream());
                 FileOutputStream fileOutputStream = new FileOutputStream(path.toString())) {
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
    }

    /**
     * Extract the dataset archive and keep the whitelisted files.
     * @param archive The archive file location.
     * @param outDir The directory to extract to.
     * @param whitelist The whitelisted files to extract.
     */
    static public void extract(Path archive, Path outDir, List<String> whitelist) {
        log.info("Starting the unarchiving…");
        try {
            FileInputStream in = new FileInputStream(archive.toString());
            InputStream bin = new BufferedInputStream(in);
            BZip2CompressorInputStream bzIn = new BZip2CompressorInputStream(bin);
            TarArchiveInputStream tarIn = new TarArchiveInputStream(bzIn);

            ArchiveEntry entry;

            if (!Files.exists(outDir)) {
                Files.createDirectory(outDir);
            }
            while (!Objects.isNull(entry = tarIn.getNextEntry())) {
                if (entry.getSize() < 1) {
                    continue;
                }
                Path entryExtractPath = Paths.get(outDir.toString(), entry.getName());
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

    }

    /**
     * Ingest the whitelisted files in the Jena database.
     * @param dbPath The path to the database.
     * @param extractedPath The directory location of extracted files.
     * @param whitelist The whitelisted files to ingest.
     */
    static public void ingest(Path dbPath, Path extractedPath, List<String> whitelist) {
        log.info("Starting to ingest in a Jena TDB2 database…");
        Dataset dataset = TDB2Factory.connectDataset(dbPath.toString());
        dataset.begin(ReadWrite.WRITE);

        for (String whitelisted : whitelist) {
            Path entryExtractPath = Paths.get(extractedPath.toString(), whitelisted);
            dataset.getDefaultModel().read(entryExtractPath.toString()); // (TODO) model: default or union ?
        }
        dataset.commit();

        dataset.end();
    }

}
