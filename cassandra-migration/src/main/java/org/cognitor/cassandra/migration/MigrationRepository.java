package org.cognitor.cassandra.migration;

import org.cognitor.cassandra.migration.collector.FailOnDuplicatesCollector;
import org.cognitor.cassandra.migration.collector.ScriptCollector;
import org.cognitor.cassandra.migration.collector.ScriptFile;
import org.cognitor.cassandra.migration.scanner.LocationScanner;
import org.cognitor.cassandra.migration.scanner.ScannerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.io.ByteStreams;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.nio.file.Files.readAllBytes;
import static java.nio.file.Paths.get;
import static java.util.Collections.sort;
import static org.cognitor.cassandra.migration.util.Ensure.notNull;
import static org.cognitor.cassandra.migration.util.Ensure.notNullOrEmpty;

/**
 * <p>
 * This class represents the collection of scripts that contain database migrations. It will scan the given location for
 * scripts that can be executed and analyzes the version of the scripts.
 * </p>
 * <p>
 * Only scripts that end with <code>SCRIPT_EXTENSION</code> will be considered.
 * </p>
 * <p>
 * Within a script every line starting with <code>COMMENT_PREFIX</code> will be ignored.
 * </p>
 *
 * @author Patrick Kranz
 */
public class MigrationRepository {
    /**
     * The default location in the classpath to check for migration scripts
     */
    public static final String DEFAULT_SCRIPT_PATH = "cassandra/migration";
    /**
     * The script extension for migrations. Every file that not ends with this extension will not be considered.
     */
    public static final String SCRIPT_EXTENSION = ".cql";

    /**
     * The encoding that es expected from the cql files.
     */
    public static final String SCRIPT_ENCODING = "UTF-8";

    /**
     * The delimiter that needs to be placed between the version and the name of the script.
     */
    public static final String VERSION_NAME_DELIMITER = "_";

    private static final Logger LOGGER = LoggerFactory.getLogger(MigrationRepository.class);
    private static final String EXTRACT_VERSION_ERROR_MSG = "Error for script %s. Unable to extract version.";
    private static final String SCANNING_SCRIPT_FOLDER_ERROR_MSG = "Error while scanning script folder for new scripts.";
    private static final String READING_SCRIPT_ERROR_MSG = "Error while reading script %s";
    private static final String PATH_SEPARATOR_CHAR = "/";

    private final ScannerRegistry scannerRegistry;
    private List<ScriptFile> migrationScripts;
    private final ScriptCollector scriptCollector;

    /**
     * Creates a new repository with the <code>DEFAULT_SCRIPT_PATH</code> configured and a
     * {@link ScriptCollector} that will throw an exception in case
     * there are duplicate versions inside the repository.
     *
     * @throws MigrationException in case there is a problem reading the scripts in the path or
     *                            the repository contains duplicate script versions
     */
    public MigrationRepository() {
        this(DEFAULT_SCRIPT_PATH);
    }

    /**
     * Creates a new repository with the given scriptPath and the default
     * {@link ScriptCollector} that will throw an exception in case
     * there are duplicate versions inside the repository.
     *
     * @param scriptPath the path on the classpath to the migration scripts. Must not be null.
     * @throws MigrationException in case there is a problem reading the scripts in the path or
     *                            the repository contains duplicate script versions
     */
    public MigrationRepository(String scriptPath) {
        this(scriptPath, new FailOnDuplicatesCollector());
    }

    /**
     * Creates a new repository with the given scriptPath and the given
     * {@link ScriptCollector}.
     *
     * @param scriptPath      the path on the classpath to the migration scripts. Must not be null.
     * @param scriptCollector the collection strategy used to collect the scripts. Must not be null.
     * @throws MigrationException in case there is a problem reading the scripts in the path.
     */
    public MigrationRepository(String scriptPath, ScriptCollector scriptCollector) {
        this(scriptPath, scriptCollector, new ScannerRegistry());
    }

    /**
     * Creates a new repository with the given scriptPath and the given
     * {@link ScriptCollector}.
     *
     * @param scriptPath      the path on the classpath to the migration scripts. Must not be null.
     * @param scriptCollector the collection strategy used to collect the scripts. Must not be null.
     * @param scannerRegistry A ScannerRegistry to create LocationScanner instances. Must not be null.
     * @throws MigrationException in case there is a problem reading the scripts in the path.
     */
    public MigrationRepository(String scriptPath, ScriptCollector scriptCollector, ScannerRegistry scannerRegistry) {
        this.scriptCollector = notNull(scriptCollector, "scriptCollector");
        this.scannerRegistry = notNull(scannerRegistry, "scannerRegistry");
        try {
            migrationScripts = scanForScripts(normalizePath(notNullOrEmpty(scriptPath, "scriptPath")));
        } catch (IOException | URISyntaxException exception) {
            throw new MigrationException(SCANNING_SCRIPT_FOLDER_ERROR_MSG, exception);
        }
    }

    /**
     * Ensures that every path starts and ends with a slash character.
     *
     * @param scriptPath the scriptPath that needs to be normalized
     * @return a path with leading and trailing slash
     */
    private String normalizePath(String scriptPath) {
        StringBuilder builder = new StringBuilder(scriptPath.length() + 1);
        if (scriptPath.startsWith("/")) {
            builder.append(scriptPath.substring(1));
        } else {
            builder.append(scriptPath);
        }
        if (!scriptPath.endsWith("/")) {
            builder.append("/");
        }
        return builder.toString();
    }

    /**
     * Gets the version of the scripts. This version represents the highest version that can be found in the scripts,
     * meaning the script with the highest version will be the one defining the version that is returned here.
     * In case the directory is empty zero will be returned as a version number.
     *
     * @return the latest version of the migrations, or zero if the directory contains no scripts.
     */
    public int getLatestVersion() {
        if (migrationScripts.isEmpty()) {
            return 0;
        }
        return migrationScripts.get(migrationScripts.size() - 1).getVersion();
    }

    private List<ScriptFile> scanForScripts(String scriptPath) throws IOException, URISyntaxException {
        LOGGER.debug("Scanning for cql migration scripts in " + scriptPath);
        Enumeration<URL> scriptResources = getClass().getClassLoader().getResources(scriptPath);
        while (scriptResources.hasMoreElements()) {
            URI script = scriptResources.nextElement().toURI();
            LOGGER.debug("Potential script folder: {}", script.toString());
            if (!scannerRegistry.supports(script.getScheme())) {
                LOGGER.debug("No LocationScanner available for scheme '{}'. Skipping it.");
                continue;
            }
            LocationScanner scanner = scannerRegistry.getScanner(script.getScheme());
            for (String resource : scanner.findResourceNames(scriptPath, script)) {
                if (isMigrationScript(resource)) {
                    String scriptName = extractScriptName(resource);
                    int version = extractScriptVersion(scriptName);
                    scriptCollector.collect(new ScriptFile(version, resource, scriptName));
                } else {
                    LOGGER.warn("Ignoring file {} because it is not a cql file.", resource);
                }
            }
        }
        List<ScriptFile> scripts = new ArrayList<>(scriptCollector.getScriptFiles());
        LOGGER.info("Found {} migration scripts", scripts.size());
        sort(scripts);
        return scripts;
    }

    private static int extractScriptVersion(String scriptName) {
        String[] splittedName = scriptName.split(VERSION_NAME_DELIMITER);
        int folderSeperatorPos = splittedName[0].lastIndexOf(PATH_SEPARATOR_CHAR);
        String versionString;
        if (folderSeperatorPos >= 0) {
            versionString = splittedName[0].substring(folderSeperatorPos + 1);
        } else {
            versionString = splittedName[0];
        }
        try {
            return parseInt(versionString);
        } catch (NumberFormatException exception) {
            throw new MigrationException(format(EXTRACT_VERSION_ERROR_MSG, scriptName),
                    exception, scriptName);
        }
    }

    private static boolean isMigrationScript(String resource) {
        return resource.endsWith(SCRIPT_EXTENSION);
    }

    private String extractScriptName(String resourceName) {
        int slashIndex = resourceName.lastIndexOf(PATH_SEPARATOR_CHAR);
        if (slashIndex > -1) {
            return resourceName.substring(slashIndex + 1);
        }
        return resourceName;
    }

    /**
     * Returns all migrations starting from given version inclusive.. Usually you want to provide the version of
     * the database plus 1 here to get all migrations that need to be executed. In case there is no script with
     * a version equal or higher than the one given, an empty list is returned.
     *
     * @param version the minimum version to be returned
     * @return all versions since the given version inclusive or an empty list if no newer script is available. Never null.
     */
    public List<DbMigration> getMigrationsSinceVersion(int version) {
        return getMigrationsSinceVersion(version, Integer.MAX_VALUE);
    }

    /**
     * Same as <code>getMigrationsSinceVersion(int version)</code> but with the difference that
     * you can specify an up until version that is excluded. So you can specify to get all version
     * from 0 to 10 which will return the scripts with version 0 to 9. The method is consistent with
     * most Java collections in the way that the start index is inclusive while the end version is
     * excluded.
     *
     * @param startVersion the minimum version to be returned.
     * @param endVersion   the first version that should not be returned anymore.
     * @return a list of DbMigration objects or an empty list in case there is no match.
     */
    public List<DbMigration> getMigrationsSinceVersion(int startVersion, int endVersion) {
        if (startVersion > endVersion) {
            throw new IllegalArgumentException("endVersion cannot be smaller than startVersion. I think you got the order of arguments wrong :)");
        }
        return migrationScripts.stream()
                .filter(isScriptWithinVersionRange(startVersion, endVersion))
                .map(script -> {
                    String content = loadScriptContent(script);
                    return new DbMigration(script.getScriptName(), script.getVersion(), content);
                })
                .collect(Collectors.toList());
    }

    private static Predicate<ScriptFile> isScriptWithinVersionRange(int version, int endVersion) {
        return script -> script.getVersion() >= version && script.getVersion() < endVersion;
    }

    private String loadScriptContent(ScriptFile script) {
        try {
            return readResourceFileAsString(script.getResourceName(), getClass().getClassLoader());
        } catch (IOException exception) {
            throw new MigrationException(format(READING_SCRIPT_ERROR_MSG, script.getResourceName()),
                    exception, script.getScriptName());
        }
    }

    private String readResourceFileAsString(String resourceName, ClassLoader classLoader) throws IOException {
        LOGGER.debug("Reading resource as string: {}", resourceName);
        try(final InputStream inputStream = classLoader.getResourceAsStream(resourceName)) {
            return new String(ByteStreams.toByteArray(inputStream), SCRIPT_ENCODING);
        } catch (Exception ie) {
            LOGGER.debug("Caught exception while trying to read resource", ie);
            throw ie;
        }
    }
}
