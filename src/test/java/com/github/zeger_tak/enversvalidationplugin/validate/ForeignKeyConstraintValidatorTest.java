package com.github.zeger_tak.enversvalidationplugin.validate;

import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.when;

import static org.junit.Assert.assertEquals;

import com.github.zeger_tak.enversvalidationplugin.connection.ConnectionProviderInstance;
import com.github.zeger_tak.enversvalidationplugin.connection.DatabaseQueries;
import com.github.zeger_tak.enversvalidationplugin.exceptions.ValidationException;
import org.dbunit.dataset.DataSetException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class ForeignKeyConstraintValidatorTest
{
	@Rule
	public MockitoRule mockitoRule = MockitoJUnit.rule();

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	@InjectMocks
	private ForeignKeyConstraintValidator validator;

	@Mock
	private ConnectionProviderInstance connectionProvider;

	@Mock
	private DatabaseQueries databaseQueries;

	@Mock
	private Map<String, String> auditTableInformationMap;

	@Before
	public void init()
	{
		when(connectionProvider.getQueries()).thenReturn(databaseQueries);
	}

	@Test
	public void testValidateNoForeignKeysExistsForTablesNotSpecifiedOnAuditTableInformationMapWithNonSpecifiedTable() throws SQLException, DataSetException
	{
		// Given
		final Set<String> tablesWithForeignKeys = Collections.singleton("not specified");
		when(databaseQueries.getListOfTablesWithForeignKeysToRevisionTable()).thenReturn(tablesWithForeignKeys);
		when(auditTableInformationMap.keySet()).thenReturn(Collections.emptySet());

		expectedException.expect(ValidationException.class);
		expectedException.expectMessage("Tables found with a reference to the revision table, which are not on the white list: [not specified]");

		// When
		validator.validateNoForeignKeysExistsForTablesNotSpecifiedOnAuditTableInformationMap();
	}

	@Test
	public void testValidateNoForeignKeysExistsForTablesNotSpecifiedOnAuditTableInformationMapWithOnlySpecifiedTables() throws SQLException, DataSetException
	{
		// Given
		final String specified = "specified";
		final Set<String> tablesWithForeignKeys = Collections.singleton(specified);
		when(auditTableInformationMap.keySet()).thenReturn(Collections.singleton(specified));
		when(databaseQueries.getListOfTablesWithForeignKeysToRevisionTable()).thenReturn(tablesWithForeignKeys);

		// When
		validator.validateNoForeignKeysExistsForTablesNotSpecifiedOnAuditTableInformationMap();
	}

	@Test
	public void testValidateNoForeignKeysExistsForTablesNotSpecifiedOnAuditTableInformationMapForWithNoTablesInDatabase() throws SQLException, DataSetException
	{
		// Given
		final Set<String> tablesWithForeignKeys = Collections.emptySet();
		when(auditTableInformationMap.keySet()).thenReturn(Collections.singleton("specified"));
		when(databaseQueries.getListOfTablesWithForeignKeysToRevisionTable()).thenReturn(tablesWithForeignKeys);

		// When
		validator.validateNoForeignKeysExistsForTablesNotSpecifiedOnAuditTableInformationMap();
	}

	@Test
	public void testValidateAllAuditTablesHaveAForeignKeyToRevisionTable() throws SQLException, DataSetException
	{
		// Given
		final Set<String> tablesWithForeignKeys = Collections.singleton("not specified");
		when(databaseQueries.getListOfTablesWithForeignKeysToRevisionTable()).thenReturn(tablesWithForeignKeys);
		when(auditTableInformationMap.keySet()).thenReturn(Collections.emptySet());

		// When
		validator.validateAllAuditTablesHaveAForeignKeyToRevisionTable();
	}

	@Test
	public void testValidateAllAuditTablesHaveAForeignKeyToRevisionTable2() throws SQLException, DataSetException
	{
		// Given
		final String specified = "specified";
		final Set<String> tablesWithForeignKeys = Collections.singleton(specified);
		when(auditTableInformationMap.keySet()).thenReturn(Collections.singleton(specified));
		when(databaseQueries.getListOfTablesWithForeignKeysToRevisionTable()).thenReturn(tablesWithForeignKeys);

		// When
		validator.validateAllAuditTablesHaveAForeignKeyToRevisionTable();
	}

	@Test
	public void testValidateAllAuditTablesHaveAForeignKeyToRevisionTable3() throws SQLException, DataSetException
	{
		// Given
		final Set<String> tablesWithForeignKeys = Collections.emptySet();
		when(auditTableInformationMap.keySet()).thenReturn(Collections.singleton("specified"));
		when(databaseQueries.getListOfTablesWithForeignKeysToRevisionTable()).thenReturn(tablesWithForeignKeys);

		try
		{
			// When
			validator.validateAllAuditTablesHaveAForeignKeyToRevisionTable();
		}
		catch (ValidationException e)
		{
			assertEquals("The following audit tables were found without a foreign key to the revision table[specified].", e.getMessage());
		}
	}
}