package fr.gdd.sage.datasets;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.tdb2.TDB2Factory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * A general class that aims to ease the opening or creation of datasets to benchmark.
 */
public class BenchmarkDataset {
    static Logger log = LoggerFactory.getLogger(BenchmarkDataset.class);

    private String defaultDbPath;
    private String dbName;
    private String archiveName;
    private String extractPath;
    private String downloadURL;

    public String dbPath_asStr;

    public BenchmarkDataset(Optional<String> dbPath_opt,
                            String defaultDbPath, String dbName, String archiveName, String extractPath,
                            String downloadURL,
                            List<String> whitelist, List<String> blacklist
                            ) {
        this.defaultDbPath = defaultDbPath;
        this.dbName = dbName;
        this.archiveName = archiveName;
        this.extractPath = extractPath;
        this.downloadURL = downloadURL;

        Path dirPath = dbPath_opt.map(Paths::get).orElseGet(() -> Paths.get(defaultDbPath));
        Path dbPath = Paths.get(dirPath.toString(), dbName);
        Path filePath = Paths.get(dirPath.toString(), archiveName);
        Path fullExtractPath = Paths.get(dirPath.toString(), extractPath);

        if (Files.exists(dbPath)) {
            log.info("Database already exists, skipping creation.");
        } else {
            log.info("Database does not exist, creating it…");
            download(filePath, downloadURL);
            extract(filePath, fullExtractPath, whitelist);
            ingest(dbPath, fullExtractPath, whitelist);
            log.info("Done with the database {}.", dbPath);
        }

        this.dbPath_asStr = dbPath.toString();

        // log.info("Reading queries…");
        // this.queries = getQueries(QUERIES_PATH, blacklist);

        // categorizeQueries(queries);
    }


    /**
     * Download the dataset from remote URL.
     * @param path The file location to download to.
     * @param url The URL of the content to download.
     */
    static public void download(Path path, String url) {
        if (!Files.exists(path)) {
            log.info("Starting the download…");

            path.toFile().getParentFile().mkdirs(); // creating parents folder if they do not exist

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
            TarArchiveInputStream tarIn = new TarArchiveInputStream(bzIn, true);


            if (!Files.exists(outDir)) {
                Files.createDirectory(outDir);
            }

            System.out.println("size  " + tarIn.getRecordSize());

            ArchiveEntry entry = tarIn.getNextEntry();
            while (!Objects.isNull(entry)) {
                if (entry.getSize() < 1) {
                    continue;
                }

                System.out.println("NAME " + entry.getName());

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

                entry = tarIn.getNextEntry();
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
        log.info("Done ingesting…");
        dataset.end();
    }
}
