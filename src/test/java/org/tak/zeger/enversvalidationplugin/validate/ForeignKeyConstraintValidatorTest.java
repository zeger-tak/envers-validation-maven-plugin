package org.tak.zeger.enversvalidationplugin.validate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.dbunit.dataset.DataSetException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.tak.zeger.enversvalidationplugin.connection.ConnectionProviderInstance;
import org.tak.zeger.enversvalidationplugin.connection.DatabaseQueries;
import org.tak.zeger.enversvalidationplugin.exceptions.ValidationException;

public class ForeignKeyConstraintValidatorTest
{
	@Rule
	public MockitoRule mockitoRule = MockitoJUnit.rule();

	@InjectMocks
	private ForeignKeyConstraintValidator validator;

	@Mock
	private ConnectionProviderInstance connectionProvider;

	@Mock
	private DatabaseQueries databaseQueries;

	@Mock
	private Map<String, String> whiteList;

	@Before
	public void init()
	{
		when(connectionProvider.getQueries()).thenReturn(databaseQueries);
	}

	@Test
	public void testValidateNoForeignKeysExistsForNonWhiteListedTablesWithNonWhiteListedTable() throws SQLException, DataSetException
	{
		// Given
		final Set<String> tablesWithForeignKeys = Collections.singleton("not whitelisted");
		when(databaseQueries.getListOfTablesWithForeignKeysToRevisionTable()).thenReturn(tablesWithForeignKeys);
		when(whiteList.keySet()).thenReturn(Collections.emptySet());

		try
		{
			// When
			validator.validateNoForeignKeysExistsForNonWhiteListedTables();
			fail("Expected a " + ValidationException.class.getSimpleName());
		}
		catch (ValidationException e)
		{
			// Then
			assertEquals("Tables found with a reference to the revision table, which are not on the white list: [not whitelisted]", e.getMessage());
		}
	}

	@Test
	public void testValidateNoForeignKeysExistsForNonWhiteListedTablesWithOnlyWhitelistedTables() throws SQLException, DataSetException
	{
		// Given
		final String whitelisted = "whitelisted";
		final Set<String> tablesWithForeignKeys = Collections.singleton(whitelisted);
		when(whiteList.keySet()).thenReturn(Collections.singleton(whitelisted));
		when(databaseQueries.getListOfTablesWithForeignKeysToRevisionTable()).thenReturn(tablesWithForeignKeys);

		// When
		validator.validateNoForeignKeysExistsForNonWhiteListedTables();
	}

	@Test
	public void testValidateNoForeignKeysExistsForNonWhiteListedTablesWithNoTablesInDatabase() throws SQLException, DataSetException
	{
		// Given
		final Set<String> tablesWithForeignKeys = Collections.emptySet();
		when(whiteList.keySet()).thenReturn(Collections.singleton("Whitelisted"));
		when(databaseQueries.getListOfTablesWithForeignKeysToRevisionTable()).thenReturn(tablesWithForeignKeys);

		// When
		validator.validateNoForeignKeysExistsForNonWhiteListedTables();
	}

	@Test
	public void testValidateAllAuditTablesHaveAForeignKeyToRevisionTable() throws SQLException, DataSetException
	{
		// Given
		final Set<String> tablesWithForeignKeys = Collections.singleton("not whitelisted");
		when(databaseQueries.getListOfTablesWithForeignKeysToRevisionTable()).thenReturn(tablesWithForeignKeys);
		when(whiteList.keySet()).thenReturn(Collections.emptySet());

		// When
		validator.validateAllAuditTablesHaveAForeignKeyToRevisionTable();
	}

	@Test
	public void testValidateAllAuditTablesHaveAForeignKeyToRevisionTable2() throws SQLException, DataSetException
	{
		// Given
		final String whitelisted = "whitelisted";
		final Set<String> tablesWithForeignKeys = Collections.singleton(whitelisted);
		when(whiteList.keySet()).thenReturn(Collections.singleton(whitelisted));
		when(databaseQueries.getListOfTablesWithForeignKeysToRevisionTable()).thenReturn(tablesWithForeignKeys);

		// When
		validator.validateAllAuditTablesHaveAForeignKeyToRevisionTable();
	}

	@Test
	public void testValidateAllAuditTablesHaveAForeignKeyToRevisionTable3() throws SQLException, DataSetException
	{
		// Given
		final Set<String> tablesWithForeignKeys = Collections.emptySet();
		when(whiteList.keySet()).thenReturn(Collections.singleton("Whitelisted"));
		when(databaseQueries.getListOfTablesWithForeignKeysToRevisionTable()).thenReturn(tablesWithForeignKeys);

		try
		{
			// When
			validator.validateAllAuditTablesHaveAForeignKeyToRevisionTable();
		}
		catch (ValidationException e)
		{
			assertEquals("Whitelisted audit tables found without a foreign key to the revision table[Whitelisted]", e.getMessage());
		}
	}
}