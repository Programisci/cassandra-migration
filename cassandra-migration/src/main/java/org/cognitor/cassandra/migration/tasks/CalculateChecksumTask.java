package org.cognitor.cassandra.migration.tasks;

import java.util.List;

import org.cognitor.cassandra.migration.Database;
import org.cognitor.cassandra.migration.DbMigration;
import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * <p>
 * This task will recalculate the checksum of the migrations in the repository
 * and update the checksums in the database. While doing this it will also
 * update the script and file name information. This task will <b>NOT</b> perform a
 * migration. You should never change an existing migration once it was executed
 * except with a new migration that will increase the version number.
 * </p>
 * <p>
 * This task is supposed to be used in situations where a new comment in the
 * script file, fixing a typo or some other trivial change will invalidate the
 * checksum in the database and therefore make the checksum validation fail.
 * </p>
 *
 * @author Patrick Kranz
 */
public class CalculateChecksumTask implements Task {
    private static final Logger LOGGER = getLogger(MigrationTask.class);

    private final Database database;

    /**
     * Creates a task that uses the given database.
     *
     * @param database   the database that should was migrated with the scripts in the repository
     */
    public CalculateChecksumTask(Database database) {
        this.database = database;
    }

    @Override
    public void execute() {
        final List<DbMigration> migrations = getMigrationsToRecalculate();
        migrations.forEach(this::updateMigration);
    }

    List<DbMigration> getMigrationsToRecalculate() {
        return database.loadMigrations();
    }

    private void updateMigration(DbMigration m) {
        m.updateChecksum();
        database.updateMigration(m);
    }
}
