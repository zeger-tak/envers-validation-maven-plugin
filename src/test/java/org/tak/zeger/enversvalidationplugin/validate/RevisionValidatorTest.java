package org.tak.zeger.enversvalidationplugin.validate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.dbunit.dataset.DataSetException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.tak.zeger.enversvalidationplugin.connection.ConnectionProviderInstance;
import org.tak.zeger.enversvalidationplugin.connection.DatabaseQueries;
import org.tak.zeger.enversvalidationplugin.entities.RevisionConstants;
import org.tak.zeger.enversvalidationplugin.entities.TableRow;
import org.tak.zeger.enversvalidationplugin.exceptions.ValidationException;

public class RevisionValidatorTest
{
	@Rule
	public MockitoRule mockitoRule = MockitoJUnit.rule();

	@Mock
	private Map<String, String> whiteList;

	@Mock
	private ConnectionProviderInstance connectionProvider;

	@Mock
	private DatabaseQueries databaseQueries;

	@Before
	public void init()
	{
		when(connectionProvider.getQueries()).thenReturn(databaseQueries);
	}

	@Test
	public void testGenerateTestDataWithEmptyWhiteList() throws SQLException, DataSetException
	{
		// Given
		when(whiteList.entrySet()).thenReturn(Collections.emptySet());

		// When
		final List<Object[]> testData = RevisionValidator.generateTestData(connectionProvider, whiteList);

		// Then
		assertTrue(testData.isEmpty());
	}

	@Test
	public void testGenerateTestData() throws SQLException, DataSetException
	{
		// Given
		final String auditTable = "";
		final String auditedTable = "";
		final List<String> primaryIdentifierColumnNames = Collections.singletonList(auditTable);

		final Map<String, List<TableRow>> auditTableRecords = Collections.singletonMap(auditTable, Collections.singletonList(new TableRow()));
		final Map<String, TableRow> auditedTableRecords = Collections.singletonMap(auditedTable, new TableRow());

		when(whiteList.entrySet()).thenReturn(Collections.singleton(new HashMap.SimpleEntry<>(auditTable, auditedTable)));
		when(databaseQueries.getPrimaryKeyColumnNames(auditedTable)).thenReturn(primaryIdentifierColumnNames);
		when(databaseQueries.getRecordInTableIdentifiedByPK(connectionProvider, auditedTable, primaryIdentifierColumnNames)).thenReturn(auditedTableRecords);
		when(databaseQueries.getRecordsInTableGroupedByPK(connectionProvider, auditTable, primaryIdentifierColumnNames)).thenReturn(auditTableRecords);

		// When
		final List<Object[]> testData = RevisionValidator.generateTestData(connectionProvider, whiteList);

		// Then
		assertEquals(1, testData.size());
		assertEquals(connectionProvider, testData.get(0)[0]);
		assertEquals(auditedTable, testData.get(0)[1]);
		assertEquals(auditedTableRecords, testData.get(0)[2]);
		assertEquals(auditTableRecords, testData.get(0)[3]);
	}

	@Test
	public void testValidateAllRecordsInAuditedTableHaveARevisionWithEmptyAuditedList()
	{
		// Given
		final String auditTableName = "auditTableName";
		final Map<String, TableRow> recordsInAuditedTable = Collections.emptyMap();
		final Map<String, List<TableRow>> recordsInAuditTable = Collections.singletonMap(auditTableName, Collections.singletonList(new TableRow()));

		final RevisionValidator validator = new RevisionValidator(connectionProvider, auditTableName, recordsInAuditedTable, recordsInAuditTable);

		// When
		validator.validatetAllRecordsInAuditedTableHaveARevision();
	}

	@Test
	public void testValidateAllRecordsInAuditedTableHaveARevisionWithEmptyAuditTable()
	{
		// Given
		final String auditTableName = "auditTableName";
		final Map<String, TableRow> recordsInAuditedTable = Collections.singletonMap(auditTableName, new TableRow());
		final Map<String, List<TableRow>> recordsInAuditTable = Collections.emptyMap();

		final RevisionValidator validator = new RevisionValidator(connectionProvider, auditTableName, recordsInAuditedTable, recordsInAuditTable);

		try
		{
			// When
			validator.validatetAllRecordsInAuditedTableHaveARevision();
			fail("Expected " + ValidationException.class.getSimpleName());
		}
		catch (ValidationException e)
		{
			assertEquals("The following identifiers [auditTableName] in table auditTableName do not have an add/update revision table as their last revision or do not have a revision at all", e.getMessage());
		}
	}

	@Test
	public void testValidateAllRecordsInAuditedTableHaveARevisionAuditTableWithoutRevColumn()
	{
		// Given
		final String auditTableName = "auditTableName";
		final String revColumnName = "revColumnName";
		final TableRow auditTableRow = mock(TableRow.class);
		final Map<String, TableRow> recordsInAuditedTable = Collections.singletonMap(auditTableName, new TableRow());
		final Map<String, List<TableRow>> recordsInAuditTable = Collections.singletonMap(auditTableName, Collections.singletonList(auditTableRow));

		when(databaseQueries.getRevTypeColumnName()).thenReturn(revColumnName);
		when(auditTableRow.getColumnValue(revColumnName)).thenReturn(null);

		final RevisionValidator validator = new RevisionValidator(connectionProvider, auditTableName, recordsInAuditedTable, recordsInAuditTable);

		// When
		validator.validatetAllRecordsInAuditedTableHaveARevision();
	}

	@Test
	public void testValidateAllRecordsInAuditedTableHaveARevisionAuditTableWithRemoveRevision()
	{
		// Given
		final String auditTableName = "auditTableName";
		final String revColumnName = "revColumnName";
		final TableRow auditTableRow = mock(TableRow.class);
		final Map<String, TableRow> recordsInAuditedTable = Collections.singletonMap(auditTableName, new TableRow());
		final Map<String, List<TableRow>> recordsInAuditTable = Collections.singletonMap(auditTableName, Collections.singletonList(auditTableRow));

		when(databaseQueries.getRevTypeColumnName()).thenReturn(revColumnName);
		when(auditTableRow.getColumnValue(revColumnName)).thenReturn(BigDecimal.valueOf(RevisionConstants.REMOVE_REVISION));

		final RevisionValidator validator = new RevisionValidator(connectionProvider, auditTableName, recordsInAuditedTable, recordsInAuditTable);

		try
		{
			// When
			validator.validatetAllRecordsInAuditedTableHaveARevision();
			fail("Expected a " + ValidationException.class.getSimpleName());
		}
		catch (ValidationException e)
		{
			assertEquals("The following identifiers [auditTableName] in table auditTableName do not have an add/update revision table as their last revision or do not have a revision at all", e.getMessage());
		}
	}

	@Test
	public void testValidateAllRecordsInAuditedTableHaveARevisionAuditTableWithAddOrUpdateRevision()
	{
		// Given
		final String auditTableName = "auditTableName";
		final String revColumnName = "revColumnName";
		final TableRow auditTableRow = mock(TableRow.class);
		final Map<String, TableRow> recordsInAuditedTable = Collections.singletonMap(auditTableName, new TableRow());
		final Map<String, List<TableRow>> recordsInAuditTable = Collections.singletonMap(auditTableName, Collections.singletonList(auditTableRow));

		when(databaseQueries.getRevTypeColumnName()).thenReturn(revColumnName);
		when(auditTableRow.getColumnValue(revColumnName)).thenReturn(BigDecimal.valueOf(RevisionConstants.ADD_REVISION));

		final RevisionValidator validator = new RevisionValidator(connectionProvider, auditTableName, recordsInAuditedTable, recordsInAuditTable);

		// When
		validator.validatetAllRecordsInAuditedTableHaveARevision();

		// Given
		when(auditTableRow.getColumnValue(revColumnName)).thenReturn(BigDecimal.valueOf(RevisionConstants.UPDATE_REVISION));

		// When
		validator.validatetAllRecordsInAuditedTableHaveARevision();
	}
}