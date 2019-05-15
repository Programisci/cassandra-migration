package org.cognitor.cassandra.migration.tasks;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import org.cognitor.cassandra.migration.Database;
import org.cognitor.cassandra.migration.DbMigration;
import org.cognitor.cassandra.migration.MigrationRepository;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class CalculateChecksumTaskTest {

    @Mock
    private Database databaseMock;

    @Mock
    private MigrationRepository repositoryMock;
    private CalculateChecksumTask task;

    @Before
    public void setUp() {
        task = new CalculateChecksumTask(databaseMock);
    }

    @Test
    public void calculatedChecksumsAreTheSameAsoriginalOnes() {
        List<DbMigration> migrations = new ArrayList<>();
        migrations.add(new DbMigration("001_test.cql", 1, "some script"));
        migrations.add(new DbMigration("002_test_again.cql", 2, "more script"));
        final List<Long> checksums = migrations.stream().map(DbMigration::getChecksum).collect(Collectors.toList());
        when(databaseMock.loadMigrations()).thenReturn(migrations);

        task.execute();

        for(int i=0; i < migrations.size(); ++i) {
            final int version = i+1, position = i;
            verify(databaseMock).updateMigration(
                    argThat(db -> db.getVersion() == version
                            && db.getScriptName().equals(migrations.get(position).getScriptName())
                            && checksums.get(position) == db.getChecksum()));
        }

    }

    @Test
    public void shouldCalculateChecksumsBeforeSaving() {
        List<DbMigration> migrations = new ArrayList<>();
        migrations.add(new DbMigration("001_test.cql", 1, "some script", 0, new Date()));
        migrations.add(new DbMigration("002_test_again.cql", 2, "more script", 0, new Date()));
        when(databaseMock.loadMigrations()).thenReturn(migrations);

        task.execute();

        verify(databaseMock, times(migrations.size())).updateMigration(argThat(dbMigration -> dbMigration.getChecksum() != 0));
    }

}
