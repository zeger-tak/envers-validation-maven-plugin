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
import org.tak.zeger.enversvalidationplugin.exceptions.ValidationException;

public class NullableColumnsValidatorTest
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
		final List<Object[]> testData = NullableColumnsValidator.generateTestData(connectionProvider, whiteList);

		// Then
		assertTrue(testData.isEmpty());
	}

	@Test
	public void testGenerateTestDataWithOneRecord() throws SQLException, DataSetException
	{
		// Given
		final String auditTable = "auditTable";
		final String auditedTable = "auditedTable";

		final List<String> pkColumnNamesAuditedTable = Collections.singletonList(auditedTable);
		final Set<String> nonNullColumns = Collections.singleton(auditTable);

		when(whiteList.entrySet()).thenReturn(Collections.singleton(new HashMap.SimpleEntry<>(auditTable, auditedTable)));
		when(databaseQueries.getPrimaryKeyColumnNames(auditedTable)).thenReturn(pkColumnNamesAuditedTable);
		when(databaseQueries.getAllNonnullColumns(auditTable)).thenReturn(nonNullColumns);

		// When
		final List<Object[]> testData = NullableColumnsValidator.generateTestData(connectionProvider, whiteList);

		// Then
		assertEquals(1, testData.size());
		assertEquals(auditedTable, testData.get(0)[0]);
		assertEquals(pkColumnNamesAuditedTable, testData.get(0)[1]);
		assertEquals(nonNullColumns, testData.get(0)[2]);
	}

	@Test
	public void testAllColumnsExceptPrimaryKeyAreNullable()
	{
		// Given
		final String tableName = "tableName";
		final List<String> pkColumnNames = Collections.singletonList(tableName);
		final Set<String> nonNullColumnNames = Collections.singleton(tableName);

		final NullableColumnsValidator validator = new NullableColumnsValidator(tableName, pkColumnNames, nonNullColumnNames);

		// When
		validator.validateAllColumnsExceptPrimaryKeyAreNullable();
	}

	@Test
	public void testAllColumnsExceptPrimaryKeyAreNullableWithOneUnexpectedNonNullColumn()
	{
		// Given
		final String tableName = "tableName";
		final List<String> pkColumnNames = Collections.singletonList(tableName);
		final String unexpectedNonNullColumnName = "unexpectedNonNullColumnName";
		final Set<String> nonNullColumnNames = new HashSet<>();
		nonNullColumnNames.add(tableName);
		nonNullColumnNames.add(unexpectedNonNullColumnName);

		final NullableColumnsValidator validator = new NullableColumnsValidator(tableName, pkColumnNames, nonNullColumnNames);

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
}