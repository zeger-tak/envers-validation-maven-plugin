package org.tak.zeger.enversvalidationplugin.validate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.dbunit.dataset.DataSetException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.tak.zeger.enversvalidationplugin.connection.ConnectionProviderInstance;
import org.tak.zeger.enversvalidationplugin.connection.DatabaseQueries;
import org.tak.zeger.enversvalidationplugin.entities.AuditTableInformation;
import org.tak.zeger.enversvalidationplugin.exceptions.ValidationException;

public class NullableColumnsValidatorTest
{
	@Rule
	public MockitoRule mockitoRule = MockitoJUnit.rule();

	@Mock
	private Map<String, AuditTableInformation> auditTableInformationMap;

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
	public void testGenerateTestDataWithEmptyAuditTableInformationMap() throws SQLException, DataSetException
	{
		// Given
		when(auditTableInformationMap.entrySet()).thenReturn(Collections.emptySet());

		// When
		final List<Object[]> testData = NullableColumnsValidator.generateTestData(connectionProvider, auditTableInformationMap);

		// Then
		assertTrue(testData.isEmpty());
	}

	@Test
	public void testGenerateTestDataWithOneRecord() throws SQLException, DataSetException
	{
		// Given
		final String auditTable = "auditTable";
		final String auditedTable = "auditedTable";
		final AuditTableInformation auditTableInformation = new AuditTableInformation(auditTable, auditedTable);

		final List<String> pkColumnNamesAuditedTable = Collections.singletonList(auditTable);
		final Set<String> nonNullColumns = Collections.singleton(auditTable);

		when(auditTableInformationMap.entrySet()).thenReturn(Collections.singleton(new HashMap.SimpleEntry<>(auditTable, auditTableInformation)));
		when(databaseQueries.getPrimaryKeyColumnNames(auditTable)).thenReturn(pkColumnNamesAuditedTable);
		when(databaseQueries.getAllNonnullColumns(auditTable)).thenReturn(nonNullColumns);

		// When
		final List<Object[]> testData = NullableColumnsValidator.generateTestData(connectionProvider, auditTableInformationMap);

		// Then
		assertEquals(1, testData.size());
		assertEquals(connectionProvider, testData.get(0)[0]);
		assertEquals(auditTableInformation, testData.get(0)[1]);
		assertEquals(pkColumnNamesAuditedTable, testData.get(0)[2]);
		assertEquals(nonNullColumns, testData.get(0)[3]);
	}

	@Test
	public void testAllColumnsExceptPrimaryKeyAreNullable()
	{
		// Given
		final String tableName = "tableName";
		final List<String> pkColumnNames = Collections.singletonList(tableName);
		final Set<String> nonNullColumnNames = Collections.singleton(tableName);
		final AuditTableInformation auditTableInformation = new AuditTableInformation(tableName, tableName);

		final NullableColumnsValidator validator = new NullableColumnsValidator(connectionProvider, auditTableInformation, pkColumnNames, nonNullColumnNames);

		// When
		validator.validateAllColumnsExceptPrimaryKeyAreNullable();
	}

	@Test
	public void testAllColumnsExceptPrimaryKeyAreNullableWithOneUnexpectedNonNullColumn()
	{
		// Given
		final String tableName = "tableName";
		final AuditTableInformation auditTableInformation = new AuditTableInformation(tableName, tableName);
		final List<String> pkColumnNames = Collections.singletonList(tableName);
		final String unexpectedNonNullColumnName = "unexpectedNonNullColumnName";
		final Set<String> nonNullColumnNames = new HashSet<>();
		nonNullColumnNames.add(tableName);
		nonNullColumnNames.add(unexpectedNonNullColumnName);

		when(databaseQueries.getRevTypeColumnName()).thenReturn("rev");

		final NullableColumnsValidator validator = new NullableColumnsValidator(connectionProvider, auditTableInformation, pkColumnNames, nonNullColumnNames);

		try
		{
			// When
			validator.validateAllColumnsExceptPrimaryKeyAreNullable();
		}
		catch (ValidationException e)
		{
			// Then
			assertEquals("The following columns for table tableName have a not null constraint which prevents remove revisions: [unexpectedNonNullColumnName]", e.getMessage());
		}
	}

	@Test
	public void testAllColumnsExceptPrimaryKeyAreNullableWithTwoUnexpectedNonNullColumns()
	{
		// Given
		final String tableName = "tableName";
		final AuditTableInformation auditTableInformation = new AuditTableInformation(tableName, tableName);
		final List<String> pkColumnNames = Collections.singletonList(tableName);
		final String unexpectedNonNullColumnName1 = "unexpectedNonNullColumnName1";
		final String unexpectedNonNullColumnName2 = "unexpectedNonNullColumnName2";
		final Set<String> nonNullColumnNames = new HashSet<>();
		nonNullColumnNames.add(tableName);
		nonNullColumnNames.add(unexpectedNonNullColumnName1);
		nonNullColumnNames.add(unexpectedNonNullColumnName2);

		when(databaseQueries.getRevTypeColumnName()).thenReturn("rev");

		final NullableColumnsValidator validator = new NullableColumnsValidator(connectionProvider, auditTableInformation, pkColumnNames, nonNullColumnNames);

		try
		{
			// When
			validator.validateAllColumnsExceptPrimaryKeyAreNullable();
		}
		catch (ValidationException e)
		{
			// Then
			assertEquals("The following columns for table tableName have a not null constraint which prevents remove revisions: [" + unexpectedNonNullColumnName1 + ", " + unexpectedNonNullColumnName2 + "]", e.getMessage());
		}
	}
}