package com.github.zeger_tak.enversvalidationplugin.validate;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.when;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.github.zeger_tak.enversvalidationplugin.connection.ConnectionProviderInstance;
import com.github.zeger_tak.enversvalidationplugin.connection.DatabaseQueries;
import com.github.zeger_tak.enversvalidationplugin.entities.AuditTableInformation;
import com.github.zeger_tak.enversvalidationplugin.exceptions.ValidationException;
import org.dbunit.dataset.DataSetException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class NullableColumnsValidatorTest
{
	@Rule
	public MockitoRule mockitoRule = MockitoJUnit.rule();

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

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
		final String contentTable = "contentTable";
		final AuditTableInformation auditTableInformation = new AuditTableInformation(auditTable, contentTable);

		final List<String> pkColumnNamesContentTable = Collections.singletonList(auditTable);
		final Set<String> nonNullColumns = Collections.singleton(auditTable);

		when(auditTableInformationMap.entrySet()).thenReturn(Collections.singleton(new HashMap.SimpleEntry<>(auditTable, auditTableInformation)));
		when(databaseQueries.getPrimaryKeyColumnNames(auditTable)).thenReturn(pkColumnNamesContentTable);
		when(databaseQueries.getAllNonnullColumns(auditTable)).thenReturn(nonNullColumns);

		// When
		final List<Object[]> testData = NullableColumnsValidator.generateTestData(connectionProvider, auditTableInformationMap);

		// Then
		assertEquals(1, testData.size());
		assertEquals(connectionProvider, testData.get(0)[0]);
		assertEquals(auditTableInformation, testData.get(0)[1]);
		assertEquals(pkColumnNamesContentTable, testData.get(0)[2]);
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

		expectedException.expect(ValidationException.class);
		expectedException.expectMessage("The following columns for table tableName have a not null constraint which prevents remove revisions: [unexpectedNonNullColumnName]");

		// When
		validator.validateAllColumnsExceptPrimaryKeyAreNullable();
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

		expectedException.expect(ValidationException.class);
		expectedException.expectMessage("The following columns for table tableName have a not null constraint which prevents remove revisions: [" + unexpectedNonNullColumnName1 + ", " + unexpectedNonNullColumnName2 + "]");

		// When
		validator.validateAllColumnsExceptPrimaryKeyAreNullable();
	}
}