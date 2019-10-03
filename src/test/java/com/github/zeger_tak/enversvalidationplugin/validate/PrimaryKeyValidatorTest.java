package com.github.zeger_tak.enversvalidationplugin.validate;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
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

public class PrimaryKeyValidatorTest
{
	private static final String AUDIT_TABLE_NAME = "auditTable";
	private static final String CONTENT_TABLE_NAME = "contentTable";
	private static final String REVISION_COLUMN_NAME = "rev";

	@Rule
	public MockitoRule mockitoRule = MockitoJUnit.rule();

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

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
	public void testGenerateTestDataWithEmptySet() throws SQLException, DataSetException
	{
		// Given
		final Map<String, AuditTableInformation> auditTableInformationMap = new HashMap<>();

		// When
		final List<Object[]> testData = PrimaryKeyValidator.generateTestData(connectionProvider, auditTableInformationMap);

		// Then
		assertTrue(testData.isEmpty());
	}

	@Test
	public void testGenerateTestData() throws SQLException, DataSetException
	{
		// Given
		final AuditTableInformation auditTableInformation = new AuditTableInformation(AUDIT_TABLE_NAME, CONTENT_TABLE_NAME);
		final Map<String, AuditTableInformation> auditTableInformationMap = Collections.singletonMap(AUDIT_TABLE_NAME, auditTableInformation);

		final List<String> pkColumnsContentTable = Collections.singletonList(CONTENT_TABLE_NAME);
		when(databaseQueries.getPrimaryKeyColumnNames(CONTENT_TABLE_NAME)).thenReturn(pkColumnsContentTable);

		final List<String> pkColumnsAuditTable = Arrays.asList(CONTENT_TABLE_NAME, AUDIT_TABLE_NAME);
		when(databaseQueries.getPrimaryKeyColumnNames(AUDIT_TABLE_NAME)).thenReturn(pkColumnsAuditTable);

		// When
		final List<Object[]> testData = PrimaryKeyValidator.generateTestData(connectionProvider, auditTableInformationMap);

		// Then
		verify(connectionProvider, atLeastOnce()).getQueries();
		assertEquals(1, testData.size());
		final Object[] testRow = testData.get(0);
		assertEquals(auditTableInformation, testRow[1]);
		assertEquals(pkColumnsAuditTable, testRow[2]);
		assertEquals(pkColumnsContentTable, testRow[3]);
	}

	@Test
	public void testValidateAuditTableHasAValidPrimaryKeyWithoutPKColumns()
	{
		// Given
		final PrimaryKeyValidator validator = new PrimaryKeyValidator(connectionProvider, new AuditTableInformation(AUDIT_TABLE_NAME, CONTENT_TABLE_NAME), Collections.emptyList(), Collections.emptyList());

		expectedException.expect(ValidationException.class);
		expectedException.expectMessage("Audit table auditTable has no primary key.");

		// When
		validator.validateAuditTableHasAValidPrimaryKey();
	}

	@Test
	public void testValidateAuditTableHasAValidPrimaryKeyWithPKColumnsOnContentTableButNoPKColumnsOnAuditTable()
	{
		// Given
		final PrimaryKeyValidator validator = new PrimaryKeyValidator(connectionProvider, new AuditTableInformation(AUDIT_TABLE_NAME, CONTENT_TABLE_NAME), Collections.singletonList(CONTENT_TABLE_NAME), Collections.emptyList());
		when(databaseQueries.getRevisionTableIdentifierColumnName()).thenReturn("rev");

		expectedException.expect(ValidationException.class);
		expectedException.expectMessage("Audit table auditTable has a primary key that is not compromised of the primary key columns of the content table [contentTable] + [rev] the following columns are missing: [rev]");

		// When
		validator.validateAuditTableHasAValidPrimaryKey();
	}

	@Test
	public void testValidateAuditTableHasAValidPrimaryKeyWithPKColumnsOnContentTableButMorePKColumnsOnAuditTable()
	{
		// Given
		final PrimaryKeyValidator validator = new PrimaryKeyValidator(connectionProvider, new AuditTableInformation(AUDIT_TABLE_NAME, CONTENT_TABLE_NAME), Arrays.asList("unexpected", REVISION_COLUMN_NAME, CONTENT_TABLE_NAME), Collections.singletonList(CONTENT_TABLE_NAME));
		when(databaseQueries.getRevisionTableIdentifierColumnName()).thenReturn(REVISION_COLUMN_NAME);

		expectedException.expect(ValidationException.class);
		expectedException.expectMessage("The primary key of audit table auditTable is comprised of more columns than expected, the following columns were not expected: [unexpected] this error may also be thrown if the content table has no primary key.");

		// When
		validator.validateAuditTableHasAValidPrimaryKey();
	}

	@Test
	public void testValidateAuditTableHasAValidPrimaryKey()
	{
		// Given
		final PrimaryKeyValidator validator = new PrimaryKeyValidator(connectionProvider, new AuditTableInformation(AUDIT_TABLE_NAME, CONTENT_TABLE_NAME), Arrays.asList(REVISION_COLUMN_NAME, CONTENT_TABLE_NAME), Collections.singletonList(CONTENT_TABLE_NAME));
		when(databaseQueries.getRevisionTableIdentifierColumnName()).thenReturn(REVISION_COLUMN_NAME);

		// When
		validator.validateAuditTableHasAValidPrimaryKey();

		// Then
		// Do nothing, result was expected
	}
}