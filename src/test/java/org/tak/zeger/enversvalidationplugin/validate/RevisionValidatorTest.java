package org.tak.zeger.enversvalidationplugin.validate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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
import org.mockito.ArgumentCaptor;
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
	private static final String AUDIT_TABLE = "auditTable";

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
		final String auditedTable = "";
		final List<String> primaryIdentifierColumnNames = Collections.singletonList(AUDIT_TABLE);

		final Map<String, List<TableRow>> auditTableRecords = Collections.singletonMap(AUDIT_TABLE, Collections.singletonList(new TableRow()));
		final Map<String, TableRow> auditedTableRecords = Collections.singletonMap(auditedTable, new TableRow());

		when(whiteList.entrySet()).thenReturn(Collections.singleton(new HashMap.SimpleEntry<>(AUDIT_TABLE, auditedTable)));
		when(databaseQueries.getPrimaryKeyColumnNames(auditedTable)).thenReturn(primaryIdentifierColumnNames);
		when(databaseQueries.getRecordInTableIdentifiedByPK(connectionProvider, auditedTable, primaryIdentifierColumnNames)).thenReturn(auditedTableRecords);
		when(databaseQueries.getRecordsInTableGroupedByPK(connectionProvider, AUDIT_TABLE, primaryIdentifierColumnNames)).thenReturn(auditTableRecords);

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
	public void testValidateAllRecordsInAuditedTableHaveAValidLatestRevisionWithEmptyAuditedList()
	{
		// Given
		final Map<String, TableRow> recordsInAuditedTable = Collections.emptyMap();
		final Map<String, List<TableRow>> recordsInAuditTable = Collections.singletonMap(AUDIT_TABLE, Collections.singletonList(new TableRow()));

		final RevisionValidator validator = new RevisionValidator(connectionProvider, AUDIT_TABLE, recordsInAuditedTable, recordsInAuditTable);

		// When
		validator.validateAllRecordsInAuditedTableHaveAValidLatestRevision();
	}

	@Test
	public void testValidateAllRecordsInAuditedTableHaveAValidLatestRevisionWithEmptyAuditTable()
	{
		// Given
		final Map<String, TableRow> recordsInAuditedTable = Collections.singletonMap(AUDIT_TABLE, new TableRow());
		final Map<String, List<TableRow>> recordsInAuditTable = Collections.emptyMap();

		final RevisionValidator validator = new RevisionValidator(connectionProvider, AUDIT_TABLE, recordsInAuditedTable, recordsInAuditTable);

		try
		{
			// When
			validator.validateAllRecordsInAuditedTableHaveAValidLatestRevision();
			fail("Expected " + ValidationException.class.getSimpleName());
		}
		catch (ValidationException e)
		{
			assertEquals("The following identifiers [auditTable] in table auditTable do not have an add/update revision table as their last revision or do not have a revision at all", e.getMessage());
		}
	}

	@Test
	public void testValidateAllRecordsInAuditedTableHaveAValidLatestRevisionAuditTableWithoutRevColumn()
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
		validator.validateAllRecordsInAuditedTableHaveAValidLatestRevision();
	}

	@Test
	public void testValidateAllRecordsInAuditedTableHaveAValidLatestRevisionAuditTableWithRemoveRevision()
	{
		// Given
		final String revColumnName = "revColumnName";
		final TableRow auditTableRow = mock(TableRow.class);
		final Map<String, TableRow> recordsInAuditedTable = Collections.singletonMap(AUDIT_TABLE, new TableRow());
		final Map<String, List<TableRow>> recordsInAuditTable = Collections.singletonMap(AUDIT_TABLE, Collections.singletonList(auditTableRow));

		when(databaseQueries.getRevTypeColumnName()).thenReturn(revColumnName);
		when(auditTableRow.getColumnValue(revColumnName)).thenReturn(BigDecimal.valueOf(RevisionConstants.REMOVE_REVISION));

		final RevisionValidator validator = spy(new RevisionValidator(connectionProvider, AUDIT_TABLE, recordsInAuditedTable, recordsInAuditTable));

		try
		{
			// When
			validator.validateAllRecordsInAuditedTableHaveAValidLatestRevision();
			fail("Expected a " + ValidationException.class.getSimpleName());
		}
		catch (ValidationException e)
		{
			assertEquals("The following identifiers [auditTable] in table auditTable do not have an add/update revision table as their last revision or do not have a revision at all", e.getMessage());
			verify(validator, never()).determineIncorrectColumns(any(TableRow.class), any(TableRow.class));
		}
	}

	@Test
	public void testValidateAllRecordsInAuditedTableHaveAValidLatestRevisionAuditTableWithValidAddRevision()
	{
		// Given
		final String revColumnName = "revColumnName";
		final TableRow actualRecord = mock(TableRow.class);
		final TableRow auditTableRow = mock(TableRow.class);
		final Map<String, TableRow> recordsInAuditedTable = Collections.singletonMap(AUDIT_TABLE, actualRecord);
		final Map<String, List<TableRow>> recordsInAuditTable = Collections.singletonMap(AUDIT_TABLE, Collections.singletonList(auditTableRow));

		final RevisionValidator validator = spy(new RevisionValidator(connectionProvider, AUDIT_TABLE, recordsInAuditedTable, recordsInAuditTable));

		when(databaseQueries.getRevTypeColumnName()).thenReturn(revColumnName);
		when(auditTableRow.getColumnValue(revColumnName)).thenReturn(BigDecimal.valueOf(RevisionConstants.ADD_REVISION));
		doReturn(Collections.emptyMap()).when(validator).determineIncorrectColumns(actualRecord, auditTableRow);

		// When
		validator.validateAllRecordsInAuditedTableHaveAValidLatestRevision();
	}

	@Test
	public void testValidateAllRecordsInAuditedTableHaveAValidLatestRevisionAuditTableWithInvalidUpdateRevision()
	{
		// Given
		final String auditTableName = "auditTableName";
		final String revColumnName = "revColumnName";
		final TableRow actualRecord = mock(TableRow.class);
		final TableRow auditTableRow = mock(TableRow.class);
		final Map<String, TableRow> recordsInAuditedTable = Collections.singletonMap(auditTableName, actualRecord);
		final Map<String, List<TableRow>> recordsInAuditTable = Collections.singletonMap(auditTableName, Collections.singletonList(auditTableRow));

		final Map<String, TableRow> incorrectColumns = mock(Map.class);

		final RevisionValidator validator = spy(new RevisionValidator(connectionProvider, auditTableName, recordsInAuditedTable, recordsInAuditTable));

		when(databaseQueries.getRevTypeColumnName()).thenReturn(revColumnName);
		when(auditTableRow.getColumnValue(revColumnName)).thenReturn(BigDecimal.valueOf(RevisionConstants.ADD_REVISION));

		doReturn(incorrectColumns).when(validator).determineIncorrectColumns(actualRecord, auditTableRow);
		doNothing().when(validator).validateLatestRevisionComparisonResult(any(), any());

		// When
		validator.validateAllRecordsInAuditedTableHaveAValidLatestRevision();

		// Then
		final ArgumentCaptor<Map> argumentCaptor = ArgumentCaptor.forClass(Map.class);
		verify(validator, times(1)).validateLatestRevisionComparisonResult(any(), argumentCaptor.capture());

		final Map invalidRevision = argumentCaptor.getValue();
		assertEquals(1, invalidRevision.size());
		assertEquals(incorrectColumns, invalidRevision.get(auditTableName));
	}

	@Test
	public void testDetermineIncorrectColumnsWithAuditTableHavingMoreColumns()
	{
		// Given
		final TableRow actualTableRow = new TableRow();
		final TableRow auditTableRow = new TableRow();
		auditTableRow.addColumn("column1", "Should not cause any problems.");

		// Method under test is not dependent on the constructor parameters
		final Map<String, TableRow> recordsInAuditedTable = Collections.emptyMap();
		final Map<String, List<TableRow>> recordsInAuditTable = Collections.emptyMap();

		final RevisionValidator validator = spy(new RevisionValidator(connectionProvider, AUDIT_TABLE, recordsInAuditedTable, recordsInAuditTable));

		// When
		final Map<String, TableRow> incorrectColumns = validator.determineIncorrectColumns(actualTableRow, auditTableRow);

		// Then
		assertTrue(incorrectColumns.isEmpty());
	}

	@Test
	public void testDetermineIncorrectColumnsWithAuditedTableHavingMoreColumns()
	{
		// Given
		final TableRow actualTableRow = new TableRow();
		actualTableRow.addColumn("column1", "Should trigger a difference.");
		final TableRow auditTableRow = new TableRow();

		// Method under test is not dependent on the constructor parameters
		final Map<String, TableRow> recordsInAuditedTable = Collections.emptyMap();
		final Map<String, List<TableRow>> recordsInAuditTable = Collections.emptyMap();

		final RevisionValidator validator = spy(new RevisionValidator(connectionProvider, AUDIT_TABLE, recordsInAuditedTable, recordsInAuditTable));

		// When
		final Map<String, TableRow> incorrectColumns = validator.determineIncorrectColumns(actualTableRow, auditTableRow);

		// Then
		assertEquals(2, incorrectColumns.size());
		final TableRow actualColumn = incorrectColumns.get("actual");
		final TableRow auditedColumn = incorrectColumns.get("audited");

		assertEquals("Should trigger a difference.", actualColumn.getColumnValue("column1"));
		assertNull(auditedColumn.getColumnValue("column1"));
	}

	@Test
	public void testDetermineIncorrectColumnsWithAuditedTableHavingSameColumnsButWithDifferentValue()
	{
		// Given
		final TableRow actualTableRow = new TableRow();
		actualTableRow.addColumn("column1", "value11");
		actualTableRow.addColumn("column2", "value12");
		final TableRow auditTableRow = new TableRow();
		auditTableRow.addColumn("column1", "value21");
		auditTableRow.addColumn("column2", null);

		// Method under test is not dependent on the constructor parameters
		final Map<String, TableRow> recordsInAuditedTable = Collections.emptyMap();
		final Map<String, List<TableRow>> recordsInAuditTable = Collections.emptyMap();

		final RevisionValidator validator = spy(new RevisionValidator(connectionProvider, AUDIT_TABLE, recordsInAuditedTable, recordsInAuditTable));

		// When
		final Map<String, TableRow> incorrectColumns = validator.determineIncorrectColumns(actualTableRow, auditTableRow);

		// Then
		assertEquals(2, incorrectColumns.size());
		final TableRow actualColumn = incorrectColumns.get("actual");
		final TableRow auditedColumn = incorrectColumns.get("audited");

		assertEquals("value11", actualColumn.getColumnValue("column1"));
		assertEquals("value21", auditedColumn.getColumnValue("column1"));
		assertEquals("value12", actualColumn.getColumnValue("column2"));
		assertNull(auditedColumn.getColumnValue("column2"));
	}

	@Test
	public void testDetermineIncorrectColumnsWithBothTablesHavingSameNumberOfColumnsButWithDifferentIds()
	{
		// Given
		final TableRow actualTableRow = new TableRow();
		actualTableRow.addColumn("column1", "value");
		final TableRow auditTableRow = new TableRow();
		auditTableRow.addColumn("column2", "value");

		// Method under test is not dependent on the constructor parameters
		final Map<String, TableRow> recordsInAuditedTable = Collections.emptyMap();
		final Map<String, List<TableRow>> recordsInAuditTable = Collections.emptyMap();

		final RevisionValidator validator = spy(new RevisionValidator(connectionProvider, AUDIT_TABLE, recordsInAuditedTable, recordsInAuditTable));

		// When
		final Map<String, TableRow> incorrectColumns = validator.determineIncorrectColumns(actualTableRow, auditTableRow);

		// Then
		assertEquals(2, incorrectColumns.size());
		final TableRow actualColumn = incorrectColumns.get("actual");
		final TableRow auditedColumn = incorrectColumns.get("audited");

		assertEquals("value", actualColumn.getColumnValue("column1"));
		assertNull(auditedColumn.getColumnValue("column1"));
		assertNull(actualColumn.getColumnValue("column2"));
		// Comparator will only provide difference check for columns present in the table to audit, and wont compare columns only present in the audit table.
		assertNull(auditedColumn.getColumnValue("column2"));
	}

	@Test
	public void testValidateLatestRevisionComparisonResultWithNomissingAddOrUpdateRevisionsAndNoRowsWithDifferentValues()
	{
		// Given
		final List<String> identifiersWhichShouldHaveAnAddOrUpdateRevision = Collections.emptyList();
		final Map<String, Map<String, TableRow>> rowsWithDifferentValues = Collections.emptyMap();

		final RevisionValidator validator = spy(new RevisionValidator(connectionProvider, AUDIT_TABLE, mock(Map.class), mock(Map.class)));

		// When
		validator.validateLatestRevisionComparisonResult(identifiersWhichShouldHaveAnAddOrUpdateRevision, rowsWithDifferentValues);
	}

	@Test
	public void testValidateLatestRevisionComparisonResultWithOneIDWithMissingRevisionAndNoRowsWithDifferentValues()
	{
		// Given
		final List<String> identifiersWhichShouldHaveAnAddOrUpdateRevision = Collections.singletonList("identifierWithMissingRevision");
		final Map<String, Map<String, TableRow>> rowsWithDifferentValues = Collections.emptyMap();

		final RevisionValidator validator = spy(new RevisionValidator(connectionProvider, AUDIT_TABLE, mock(Map.class), mock(Map.class)));

		try
		{
			// When
			validator.validateLatestRevisionComparisonResult(identifiersWhichShouldHaveAnAddOrUpdateRevision, rowsWithDifferentValues);
			fail("Expected " + ValidationException.class.getSimpleName());
		}
		catch (ValidationException e)
		{
			assertEquals("The following identifiers [identifierWithMissingRevision] in table auditTable do not have an add/update revision table as their last revision or do not have a revision at all", e.getMessage());
		}
	}

	@Test
	public void testValidateLatestRevisionComparisonResultWithOneIDWithMissingRevisionAndOneRowWithDifferentValues()
	{
		// Given
		final List<String> identifiersWhichShouldHaveAnAddOrUpdateRevision = Collections.singletonList("identifierWithMissingRevision");

		final TableRow actualTableRow = new TableRow();
		actualTableRow.addColumn("column", "actualValue");
		final TableRow auditTableRow = new TableRow();
		auditTableRow.addColumn("column", "auditValue");

		final Map<String, TableRow> differentColumns = new HashMap<>();
		differentColumns.put("actual", actualTableRow);
		differentColumns.put("audited", auditTableRow);
		final Map<String, Map<String, TableRow>> rowsWithDifferentValues = Collections.singletonMap("identifierWithDifferentAudit", differentColumns);

		final RevisionValidator validator = spy(new RevisionValidator(connectionProvider, AUDIT_TABLE, mock(Map.class), mock(Map.class)));

		try
		{
			// When
			validator.validateLatestRevisionComparisonResult(identifiersWhichShouldHaveAnAddOrUpdateRevision, rowsWithDifferentValues);
			fail("Expected " + ValidationException.class.getSimpleName());
		}
		catch (ValidationException e)
		{
			assertEquals(
					//@formatter:off
				"The following identifiers [identifierWithMissingRevision] in table auditTable do not have an add/update revision table as their last revision or do not have a revision at all. \n" +
						"Row with identifier identifierWithDifferentAudit has a different audit row than the actual value in the table to audit, the following columns differ: \n" +
						"\tActual value for column column: actualValue, audited value: auditValue.\n", e.getMessage());
			//@formatter:on
		}
	}

	@Test
	public void testValidateLatestRevisionComparisonResultWithNoIDWithMissingRevisionAndOneRowWithDifferentValues()
	{
		// Given
		final List<String> identifiersWhichShouldHaveAnAddOrUpdateRevision = Collections.emptyList();

		final TableRow actualTableRow = new TableRow();
		actualTableRow.addColumn("column", "actualValue");
		final TableRow auditTableRow = new TableRow();
		auditTableRow.addColumn("column", "auditValue");

		final Map<String, TableRow> differentColumns = new HashMap<>();
		differentColumns.put("actual", actualTableRow);
		differentColumns.put("audited", auditTableRow);
		final Map<String, Map<String, TableRow>> rowsWithDifferentValues = Collections.singletonMap("identifierWithDifferentAudit", differentColumns);

		final RevisionValidator validator = spy(new RevisionValidator(connectionProvider, AUDIT_TABLE, mock(Map.class), mock(Map.class)));

		try
		{
			// When
			validator.validateLatestRevisionComparisonResult(identifiersWhichShouldHaveAnAddOrUpdateRevision, rowsWithDifferentValues);
			fail("Expected " + ValidationException.class.getSimpleName());
		}
		catch (ValidationException e)
		{
			assertEquals(
					//@formatter:off
				"Row with identifier identifierWithDifferentAudit has a different audit row than the actual value in the table to audit, the following columns differ: \n" +
						"\tActual value for column column: actualValue, audited value: auditValue.\n", e.getMessage());
			//@formatter:on
		}
	}

	@Test
	public void testValidateHistoryIsAValidFlowWithEmptyAuditTable()
	{
		// Given
		final Map<String, TableRow> recordsInAuditedTable = Collections.singletonMap(AUDIT_TABLE, new TableRow());
		final Map<String, List<TableRow>> recordsInAuditTable = Collections.emptyMap();
		final RevisionValidator validator = spy(new RevisionValidator(connectionProvider, AUDIT_TABLE, recordsInAuditedTable, recordsInAuditTable));

		// When
		validator.validateHistoryIsAValidFlow();
	}
}