package com.github.zeger_tak.enversvalidationplugin.validate;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.mockito.Matchers.any;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.github.zeger_tak.enversvalidationplugin.connection.ConnectionProviderInstance;
import com.github.zeger_tak.enversvalidationplugin.connection.DatabaseQueries;
import com.github.zeger_tak.enversvalidationplugin.entities.AuditTableInformation;
import com.github.zeger_tak.enversvalidationplugin.entities.RevisionConstants;
import com.github.zeger_tak.enversvalidationplugin.entities.TableRow;
import com.github.zeger_tak.enversvalidationplugin.exceptions.ValidationException;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.DataSetException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class RevisionValidatorTest
{
	private static final String AUDIT_TABLE = "auditTable";

	@Rule
	public MockitoRule mockitoRule = MockitoJUnit.rule();

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	@Mock
	private Map<String, AuditTableInformation> auditTableInformationMap;

	@Mock
	private ConnectionProviderInstance connectionProvider;

	@Mock
	private IDatabaseConnection databaseConnection;

	@Mock
	private DatabaseQueries databaseQueries;

	@Before
	public void init()
	{
		when(connectionProvider.getQueries()).thenReturn(databaseQueries);
		when(connectionProvider.getDatabaseConnection()).thenReturn(databaseConnection);
	}

	@Test
	public void testGenerateTestDataWithEmptyAuditTableInformationMap() throws SQLException, DataSetException
	{
		// Given
		when(auditTableInformationMap.entrySet()).thenReturn(Collections.emptySet());

		// When
		final List<Object[]> testData = RevisionValidator.generateTestData(connectionProvider, auditTableInformationMap);

		// Then
		assertTrue(testData.isEmpty());
	}

	@Test
	public void testGenerateTestData() throws SQLException, DataSetException
	{
		// Given
		final String contentTable = "";
		final AuditTableInformation auditTableInformation = new AuditTableInformation(AUDIT_TABLE, contentTable);
		final List<String> primaryIdentifierColumnNames = Collections.singletonList(AUDIT_TABLE);

		final Map<String, List<TableRow>> auditTableRecords = Collections.singletonMap(AUDIT_TABLE, Collections.singletonList(new TableRow()));
		final Map<String, TableRow> contentTableRecords = Collections.singletonMap(contentTable, new TableRow());

		when(auditTableInformationMap.entrySet()).thenReturn(Collections.singleton(new HashMap.SimpleEntry<>(AUDIT_TABLE, new AuditTableInformation(AUDIT_TABLE, contentTable))));
		when(databaseQueries.getPrimaryKeyColumnNames(contentTable)).thenReturn(primaryIdentifierColumnNames);
		when(databaseQueries.getContentRecords(databaseConnection, auditTableInformation, primaryIdentifierColumnNames)).thenReturn(contentTableRecords);
		when(databaseQueries.getAuditRecordsGroupedByContentPrimaryKey(databaseConnection, auditTableInformation, primaryIdentifierColumnNames)).thenReturn(auditTableRecords);

		// When
		final List<Object[]> testData = RevisionValidator.generateTestData(connectionProvider, auditTableInformationMap);

		// Then
		assertEquals(1, testData.size());
		assertEquals(connectionProvider, testData.get(0)[0]);
		assertEquals(contentTable, ((AuditTableInformation) testData.get(0)[1]).getContentTableName());
		assertEquals(auditTableRecords, testData.get(0)[2]);
		assertEquals(contentTableRecords, testData.get(0)[3]);
	}

	@Test
	public void testValidateAllRecordsInContentTableHaveAValidLatestRevisionWithEmptyContentList()
	{
		// Given
		final Map<String, TableRow> recordsInContentTable = Collections.emptyMap();
		final Map<String, List<TableRow>> recordsInAuditTable = Collections.singletonMap(AUDIT_TABLE, Collections.singletonList(new TableRow()));
		final AuditTableInformation auditTableInformation = new AuditTableInformation(AUDIT_TABLE, AUDIT_TABLE);

		final RevisionValidator validator = new RevisionValidator(connectionProvider, auditTableInformation, recordsInAuditTable, recordsInContentTable);

		// When
		validator.validateAllRecordsInContentTableHaveAValidLatestRevision();
	}

	@Test
	public void testValidateAllRecordsInContentTableHaveAValidLatestRevisionWithEmptyAuditTable()
	{
		// Given
		final Map<String, TableRow> recordsInContentTable = Collections.singletonMap(AUDIT_TABLE, new TableRow());
		final Map<String, List<TableRow>> recordsInAuditTable = Collections.emptyMap();
		final AuditTableInformation auditTableInformation = new AuditTableInformation(AUDIT_TABLE, AUDIT_TABLE);

		final RevisionValidator validator = new RevisionValidator(connectionProvider, auditTableInformation, recordsInAuditTable, recordsInContentTable);

		expectedException.expect(ValidationException.class);
		expectedException.expectMessage("The following identifiers [auditTable] in table auditTable do not have an Add/Modify revision in table auditTable as their last revision or do not have a revision at all.");

		// When
		validator.validateAllRecordsInContentTableHaveAValidLatestRevision();
	}

	@Test
	public void testValidateAllRecordsInContentTableHaveAValidLatestRevisionAuditTableWithoutRevColumn()
	{
		// Given
		final AuditTableInformation auditTableInformation = new AuditTableInformation(AUDIT_TABLE, AUDIT_TABLE);
		final String revColumnName = "revColumnName";
		final TableRow auditTableRow = mock(TableRow.class);
		final Map<String, TableRow> recordsInContentTable = Collections.singletonMap(AUDIT_TABLE, new TableRow());
		final Map<String, List<TableRow>> recordsInAuditTable = Collections.singletonMap(AUDIT_TABLE, Collections.singletonList(auditTableRow));

		when(databaseQueries.getRevTypeColumnName()).thenReturn(revColumnName);
		when(auditTableRow.getColumnValue(revColumnName)).thenReturn(null);

		final RevisionValidator validator = new RevisionValidator(connectionProvider, auditTableInformation, recordsInAuditTable, recordsInContentTable);

		expectedException.expect(ValidationException.class);
		expectedException.expectMessage("The audit table auditTable does not have a column referring to the revision table.");

		// When
		validator.validateAllRecordsInContentTableHaveAValidLatestRevision();
	}

	@Test
	public void testValidateAllRecordsInContentTableHaveAValidLatestRevisionAuditTableWithRemoveRevision()
	{
		// Given
		final String revColumnName = "revColumnName";
		final TableRow auditTableRow = mock(TableRow.class);
		final Map<String, TableRow> recordsInContentTable = Collections.singletonMap(AUDIT_TABLE, new TableRow());
		final Map<String, List<TableRow>> recordsInAuditTable = Collections.singletonMap(AUDIT_TABLE, Collections.singletonList(auditTableRow));
		final AuditTableInformation auditTableInformation = new AuditTableInformation(AUDIT_TABLE, AUDIT_TABLE);

		when(databaseQueries.getRevTypeColumnName()).thenReturn(revColumnName);
		when(auditTableRow.getColumnValue(revColumnName)).thenReturn(BigDecimal.valueOf(RevisionConstants.REMOVE_REVISION));

		final RevisionValidator validator = spy(new RevisionValidator(connectionProvider, auditTableInformation, recordsInAuditTable, recordsInContentTable));

		try
		{
			// When
			validator.validateAllRecordsInContentTableHaveAValidLatestRevision();
			fail("Expected a " + ValidationException.class.getSimpleName());
		}
		catch (ValidationException e)
		{
			assertEquals("The following identifiers [auditTable] in table auditTable do not have an Add/Modify revision in table auditTable as their last revision or do not have a revision at all.", e.getMessage());
			verify(validator, never()).determineIncorrectColumns(any(TableRow.class), any(TableRow.class));
		}
	}

	@Test
	public void testValidateAllRecordsInContentTableHaveAValidLatestRevisionAuditTableWithValidAddRevision()
	{
		// Given
		final String revColumnName = "revColumnName";
		final TableRow actualRecord = mock(TableRow.class);
		final TableRow auditTableRow = mock(TableRow.class);
		final Map<String, TableRow> recordsInContentTable = Collections.singletonMap(AUDIT_TABLE, actualRecord);
		final Map<String, List<TableRow>> recordsInAuditTable = Collections.singletonMap(AUDIT_TABLE, Collections.singletonList(auditTableRow));
		final AuditTableInformation auditTableInformation = new AuditTableInformation(AUDIT_TABLE, AUDIT_TABLE);

		final RevisionValidator validator = spy(new RevisionValidator(connectionProvider, auditTableInformation, recordsInAuditTable, recordsInContentTable));

		when(databaseQueries.getRevTypeColumnName()).thenReturn(revColumnName);
		when(auditTableRow.getColumnValue(revColumnName)).thenReturn(BigDecimal.valueOf(RevisionConstants.ADD_REVISION));
		doReturn(Collections.emptyMap()).when(validator).determineIncorrectColumns(actualRecord, auditTableRow);

		// When
		validator.validateAllRecordsInContentTableHaveAValidLatestRevision();
	}

	@Test
	public void testValidateAllRecordsInContentTableHaveAValidLatestRevisionAuditTableWithInvalidModifyRevision()
	{
		// Given
		final AuditTableInformation auditTableInformation = new AuditTableInformation(AUDIT_TABLE, AUDIT_TABLE);
		final String revColumnName = "revColumnName";
		final TableRow actualRecord = mock(TableRow.class);
		final TableRow auditTableRow = mock(TableRow.class);
		final Map<String, TableRow> recordsInContentTable = Collections.singletonMap(AUDIT_TABLE, actualRecord);
		final Map<String, List<TableRow>> recordsInAuditTable = Collections.singletonMap(AUDIT_TABLE, Collections.singletonList(auditTableRow));

		final Map<String, TableRow> incorrectColumns = mock(Map.class);

		final RevisionValidator validator = spy(new RevisionValidator(connectionProvider, auditTableInformation, recordsInAuditTable, recordsInContentTable));

		when(databaseQueries.getRevTypeColumnName()).thenReturn(revColumnName);
		when(auditTableRow.getColumnValue(revColumnName)).thenReturn(BigDecimal.valueOf(RevisionConstants.ADD_REVISION));

		doReturn(incorrectColumns).when(validator).determineIncorrectColumns(actualRecord, auditTableRow);
		doNothing().when(validator).validateLatestRevisionComparisonResult(any(), any());

		// When
		validator.validateAllRecordsInContentTableHaveAValidLatestRevision();

		// Then
		final ArgumentCaptor<Map> argumentCaptor = ArgumentCaptor.forClass(Map.class);
		verify(validator, times(1)).validateLatestRevisionComparisonResult(any(), argumentCaptor.capture());

		final Map invalidRevision = argumentCaptor.getValue();
		assertEquals(1, invalidRevision.size());
		assertEquals(incorrectColumns, invalidRevision.get(AUDIT_TABLE));
	}

	@Test
	public void testDetermineIncorrectColumnsWithAuditTableHavingMoreColumns()
	{
		// Given
		final TableRow actualTableRow = new TableRow();
		final TableRow auditTableRow = new TableRow();
		auditTableRow.addColumn("column1", "Should not cause any problems.");

		// Method under test is not dependent on the constructor parameters
		final Map<String, TableRow> recordsInContentTable = Collections.emptyMap();
		final Map<String, List<TableRow>> recordsInAuditTable = Collections.emptyMap();
		final AuditTableInformation auditTableInformation = new AuditTableInformation(AUDIT_TABLE, AUDIT_TABLE);

		final RevisionValidator validator = spy(new RevisionValidator(connectionProvider, auditTableInformation, recordsInAuditTable, recordsInContentTable));

		// When
		final Map<String, TableRow> incorrectColumns = validator.determineIncorrectColumns(actualTableRow, auditTableRow);

		// Then
		assertTrue(incorrectColumns.isEmpty());
	}

	@Test
	public void testDetermineIncorrectColumnsWithContentTableHavingMoreColumns()
	{
		// Given
		final TableRow actualTableRow = new TableRow();
		actualTableRow.addColumn("column1", "Should trigger a difference.");
		final TableRow auditTableRow = new TableRow();

		// Method under test is not dependent on the constructor parameters
		final Map<String, TableRow> recordsInContentTable = Collections.emptyMap();
		final Map<String, List<TableRow>> recordsInAuditTable = Collections.emptyMap();
		final AuditTableInformation auditTableInformation = new AuditTableInformation(AUDIT_TABLE, AUDIT_TABLE);

		final RevisionValidator validator = spy(new RevisionValidator(connectionProvider, auditTableInformation, recordsInAuditTable, recordsInContentTable));

		// When
		final Map<String, TableRow> incorrectColumns = validator.determineIncorrectColumns(actualTableRow, auditTableRow);

		// Then
		assertEquals(2, incorrectColumns.size());
		final TableRow actualColumn = incorrectColumns.get("actual");
		final TableRow ContentColumn = incorrectColumns.get("audit");

		assertEquals("Should trigger a difference.", actualColumn.getColumnValue("column1"));
		assertNull(ContentColumn.getColumnValue("column1"));
	}

	@Test
	public void testDetermineIncorrectColumnsWithContentTableHavingSameColumnsButWithDifferentValue()
	{
		// Given
		final TableRow actualTableRow = new TableRow();
		actualTableRow.addColumn("column1", "value11");
		actualTableRow.addColumn("column2", "value12");
		final TableRow auditTableRow = new TableRow();
		auditTableRow.addColumn("column1", "value21");
		auditTableRow.addColumn("column2", null);

		// Method under test is not dependent on the constructor parameters
		final Map<String, TableRow> recordsInContentTable = Collections.emptyMap();
		final Map<String, List<TableRow>> recordsInAuditTable = Collections.emptyMap();
		final AuditTableInformation auditTableInformation = new AuditTableInformation(AUDIT_TABLE, AUDIT_TABLE);

		final RevisionValidator validator = spy(new RevisionValidator(connectionProvider, auditTableInformation, recordsInAuditTable, recordsInContentTable));

		// When
		final Map<String, TableRow> incorrectColumns = validator.determineIncorrectColumns(actualTableRow, auditTableRow);

		// Then
		assertEquals(2, incorrectColumns.size());
		final TableRow actualColumn = incorrectColumns.get("actual");
		final TableRow ContentColumn = incorrectColumns.get("audit");

		assertEquals("value11", actualColumn.getColumnValue("column1"));
		assertEquals("value21", ContentColumn.getColumnValue("column1"));
		assertEquals("value12", actualColumn.getColumnValue("column2"));
		assertNull(ContentColumn.getColumnValue("column2"));
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
		final Map<String, TableRow> recordsInContentTable = Collections.emptyMap();
		final Map<String, List<TableRow>> recordsInAuditTable = Collections.emptyMap();
		final AuditTableInformation auditTableInformation = new AuditTableInformation(AUDIT_TABLE, AUDIT_TABLE);

		final RevisionValidator validator = spy(new RevisionValidator(connectionProvider, auditTableInformation, recordsInAuditTable, recordsInContentTable));

		// When
		final Map<String, TableRow> incorrectColumns = validator.determineIncorrectColumns(actualTableRow, auditTableRow);

		// Then
		assertEquals(2, incorrectColumns.size());
		final TableRow actualColumn = incorrectColumns.get("actual");
		final TableRow ContentColumn = incorrectColumns.get("audit");

		assertEquals("value", actualColumn.getColumnValue("column1"));
		assertNull(ContentColumn.getColumnValue("column1"));
		assertNull(actualColumn.getColumnValue("column2"));
		// Comparator will only provide difference check for columns present in the content table, and wont compare columns only present in the audit table.
		assertNull(ContentColumn.getColumnValue("column2"));
	}

	@Test
	public void testValidateLatestRevisionComparisonResultWithNomissingAddOrModifyRevisionsAndNoRowsWithDifferentValues()
	{
		// Given
		final List<String> identifiersWhichShouldHaveAnAddOrModifyRevision = Collections.emptyList();
		final Map<String, Map<String, TableRow>> rowsWithDifferentValues = Collections.emptyMap();
		final AuditTableInformation auditTableInformation = new AuditTableInformation(AUDIT_TABLE, AUDIT_TABLE);

		final RevisionValidator validator = spy(new RevisionValidator(connectionProvider, auditTableInformation, mock(Map.class), mock(Map.class)));

		// When
		validator.validateLatestRevisionComparisonResult(identifiersWhichShouldHaveAnAddOrModifyRevision, rowsWithDifferentValues);
	}

	@Test
	public void testValidateLatestRevisionComparisonResultWithOneIDWithMissingRevisionAndNoRowsWithDifferentValues()
	{
		// Given
		final List<String> identifiersWhichShouldHaveAnAddOrModifyRevision = Collections.singletonList("identifierWithMissingRevision");
		final Map<String, Map<String, TableRow>> rowsWithDifferentValues = Collections.emptyMap();
		final AuditTableInformation auditTableInformation = new AuditTableInformation(AUDIT_TABLE, AUDIT_TABLE);

		final RevisionValidator validator = spy(new RevisionValidator(connectionProvider, auditTableInformation, mock(Map.class), mock(Map.class)));

		expectedException.expect(ValidationException.class);
		expectedException.expectMessage("The following identifiers [identifierWithMissingRevision] in table auditTable do not have an Add/Modify revision in table auditTable as their last revision or do not have a revision at all.");

		// When
		validator.validateLatestRevisionComparisonResult(identifiersWhichShouldHaveAnAddOrModifyRevision, rowsWithDifferentValues);
	}

	@Test
	public void testValidateLatestRevisionComparisonResultWithOneIDWithMissingRevisionAndOneRowWithDifferentValues()
	{
		// Given
		final List<String> identifiersWhichShouldHaveAnAddOrModifyRevision = Collections.singletonList("identifierWithMissingRevision");

		final TableRow actualTableRow = new TableRow();
		actualTableRow.addColumn("column", "actualValue");
		final TableRow auditTableRow = new TableRow();
		auditTableRow.addColumn("column", "auditValue");

		final Map<String, TableRow> differentColumns = new HashMap<>();
		differentColumns.put("actual", actualTableRow);
		differentColumns.put("audit", auditTableRow);
		final Map<String, Map<String, TableRow>> rowsWithDifferentValues = Collections.singletonMap("identifierWithDifferentAudit", differentColumns);
		final AuditTableInformation auditTableInformation = new AuditTableInformation(AUDIT_TABLE, AUDIT_TABLE);

		final RevisionValidator validator = spy(new RevisionValidator(connectionProvider, auditTableInformation, mock(Map.class), mock(Map.class)));

		//@formatter:off
		final String expectedMessage = "The following identifiers [identifierWithMissingRevision] in table auditTable do not have an Add/Modify revision in table auditTable as their last revision or do not have a revision at all.\n" +
				"Row with identifier identifierWithDifferentAudit has a different audit row than the actual value in the content table, the following columns differ: \n" +
				"\tActual value for column column: actualValue, audit value: auditValue.\n";
		//@formatter:on

		expectedException.expect(ValidationException.class);
		expectedException.expectMessage(expectedMessage);

		// When
		validator.validateLatestRevisionComparisonResult(identifiersWhichShouldHaveAnAddOrModifyRevision, rowsWithDifferentValues);
	}

	@Test
	public void testValidateLatestRevisionComparisonResultWithNoIDWithMissingRevisionAndOneRowWithDifferentValues()
	{
		// Given
		final List<String> identifiersWhichShouldHaveAnAddOrModifyRevision = Collections.emptyList();

		final TableRow actualTableRow = new TableRow();
		actualTableRow.addColumn("column", "actualValue");
		final TableRow auditTableRow = new TableRow();
		auditTableRow.addColumn("column", "auditValue");

		final Map<String, TableRow> differentColumns = new HashMap<>();
		differentColumns.put("actual", actualTableRow);
		differentColumns.put("audit", auditTableRow);
		final Map<String, Map<String, TableRow>> rowsWithDifferentValues = Collections.singletonMap("identifierWithDifferentAudit", differentColumns);
		final AuditTableInformation auditTableInformation = new AuditTableInformation(AUDIT_TABLE, AUDIT_TABLE);

		final RevisionValidator validator = spy(new RevisionValidator(connectionProvider, auditTableInformation, mock(Map.class), mock(Map.class)));

		//@formatter:off
		final String expectedMessage = "Row with identifier identifierWithDifferentAudit has a different audit row than the actual value in the content table, the following columns differ: \n" +
					"\tActual value for column column: actualValue, audit value: auditValue.\n";
		//@formatter:on

		expectedException.expect(ValidationException.class);
		expectedException.expectMessage(expectedMessage);

		// When
		validator.validateLatestRevisionComparisonResult(identifiersWhichShouldHaveAnAddOrModifyRevision, rowsWithDifferentValues);
	}

	@Test
	public void testValidateHistoryIsAValidFlowWithEmptyAuditTable()
	{
		// Given
		final Map<String, List<TableRow>> recordsInAuditTable = Collections.emptyMap();
		final Map<String, TableRow> recordsInContentTable = Collections.emptyMap();
		final RevisionValidator validator = spy(new RevisionValidator(connectionProvider, new AuditTableInformation(AUDIT_TABLE, AUDIT_TABLE), recordsInAuditTable, recordsInContentTable));

		// When
		validator.validateHistoryIsAValidFlow();
	}
}