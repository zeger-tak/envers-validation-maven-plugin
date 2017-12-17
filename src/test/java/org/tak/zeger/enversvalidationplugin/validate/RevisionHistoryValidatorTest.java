package org.tak.zeger.enversvalidationplugin.validate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

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
import org.tak.zeger.enversvalidationplugin.entities.TableRow;
import org.tak.zeger.enversvalidationplugin.entities.WhitelistEntry;

public class RevisionHistoryValidatorTest
{
	private static final String AUDIT_TABLE = "auditTable";

	@Rule
	public MockitoRule mockitoRule = MockitoJUnit.rule();

	@Mock
	private Map<String, WhitelistEntry> whiteList;

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
		final List<Object[]> testData = RevisionHistoryValidator.generateTestData(connectionProvider, whiteList);

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

		when(whiteList.entrySet()).thenReturn(Collections.singleton(new HashMap.SimpleEntry<>(AUDIT_TABLE, new WhitelistEntry(AUDIT_TABLE, null, auditedTable))));
		when(databaseQueries.getPrimaryKeyColumnNames(auditedTable)).thenReturn(primaryIdentifierColumnNames);
		when(databaseQueries.getRecordsInTableGroupedByPK(connectionProvider, AUDIT_TABLE, primaryIdentifierColumnNames)).thenReturn(auditTableRecords);

		// When
		final List<Object[]> testData = RevisionHistoryValidator.generateTestData(connectionProvider, whiteList);

		// Then
		assertEquals(1, testData.size());
		assertEquals(connectionProvider, testData.get(0)[0]);
		assertEquals(auditedTable, ((WhitelistEntry) testData.get(0)[1]).getContentTableName());
		assertEquals(auditTableRecords, testData.get(0)[2]);
	}

	@Test
	public void testValidateHistoryIsAValidFlowWithEmptyAuditTable()
	{
		// Given
		final Map<String, List<TableRow>> recordsInAuditTable = Collections.emptyMap();
		final Map<String, TableRow> recordsInAuditedTable = Collections.emptyMap();
		final RevisionHistoryValidator validator = spy(new RevisionHistoryValidator(connectionProvider, new WhitelistEntry(AUDIT_TABLE, null, AUDIT_TABLE), recordsInAuditTable, recordsInAuditedTable));

		// When
		validator.validateHistoryIsAValidFlow();
	}
}