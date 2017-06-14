package org.tak.zeger.enversvalidationplugin.validate;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.dbunit.database.CachedResultSetTable;
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

public class AuditTableWhiteListValidatorTest
{
	@Rule
	public MockitoRule mockitoRule = MockitoJUnit.rule();

	@InjectMocks
	private AuditTableWhiteListValidator validator;

	@Mock
	private Map<String, String> whiteList;

	@Mock
	private Set<String> auditTablesInDatabase;

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
	public void testValidateAllExistingAuditTablesAreWhiteListedAndWhiteListedTablesThatDoNotExist()
	{
		// Given
		final Set<String> existingTables = Collections.emptySet();
		final String whiteListedTable = "whiteListed";

		when(auditTablesInDatabase.stream()).thenReturn(existingTables.stream());
		when(whiteList.keySet()).thenReturn(Collections.singleton(whiteListedTable));

		// When
		validator.validateAllExistingAuditTablesAreWhiteListed();
	}

	@Test
	public void testValidateAllExistingAuditTablesAreWhiteListedAndAllWhiteListedTablesExist()
	{
		// Given
		final String whiteListedTable = "whiteListed";
		final Set<String> existingTables = Collections.singleton(whiteListedTable);

		when(auditTablesInDatabase.stream()).thenReturn(existingTables.stream());
		when(whiteList.keySet()).thenReturn(Collections.singleton(whiteListedTable.toUpperCase()));

		// When
		validator.validateAllExistingAuditTablesAreWhiteListed();
	}

	@Test
	public void testValidateAllExistingAuditTablesAreWhiteListedAndSomeExistingTablesAreNotWhiteListed()
	{
		// Given
		final String whiteListedTable = "whiteListed";
		final String tableNotOnWhiteList = "unlisted";
		final List<String> existingTables = Arrays.asList(whiteListedTable, tableNotOnWhiteList);

		when(auditTablesInDatabase.stream()).thenReturn(existingTables.stream());
		when(whiteList.keySet()).thenReturn(Collections.singleton(whiteListedTable.toUpperCase()));

		try
		{
			// When
			validator.validateAllExistingAuditTablesAreWhiteListed();
		}
		catch (ValidationException e)
		{
			assertEquals("The following audit tables are not whitelisted: [UNLISTED]", e.getMessage());
		}
	}

	@Test
	public void testValidateAllWhiteListedAuditTablesExistAndWhiteListedTablesThatDoNotExist()
	{
		// Given
		final Set<String> existingTables = Collections.emptySet();
		final String whiteListedTable = "whiteListed";

		when(auditTablesInDatabase.stream()).thenReturn(existingTables.stream());
		when(whiteList.keySet()).thenReturn(Collections.singleton(whiteListedTable));

		try
		{
			// When
			validator.validateAllWhiteListedAuditTablesExist();
		}
		catch (ValidationException e)
		{
			assertEquals("The following whitelisted tables do not exist in the database: [whiteListed]", e.getMessage());
		}
	}

	@Test
	public void testValidateAllWhiteListedAuditTablesExistAndAllWhiteListedTablesExist()
	{
		// Given
		final String whiteListedTable = "whiteListed";
		final Set<String> existingTables = Collections.singleton(whiteListedTable);

		when(auditTablesInDatabase.stream()).thenReturn(existingTables.stream());
		when(whiteList.keySet()).thenReturn(Collections.singleton(whiteListedTable.toUpperCase()));

		// When
		validator.validateAllWhiteListedAuditTablesExist();
	}

	@Test
	public void testValidateAllWhiteListedAuditTablesExistAndSomeExistingTablesAreNotWhiteListed()
	{
		// Given
		final String whiteListedTable = "whiteListed";
		final String tableNotOnWhiteList = "unlisted";
		final List<String> existingTables = Arrays.asList(whiteListedTable, tableNotOnWhiteList);

		when(auditTablesInDatabase.stream()).thenReturn(existingTables.stream());
		when(whiteList.keySet()).thenReturn(Collections.singleton(whiteListedTable.toUpperCase()));

		// When
		validator.validateAllWhiteListedAuditTablesExist();
	}

	@Test
	public void testValidateAllWhiteListedAuditTablesAuditAnExistingTableWithEmptyWhiteList() throws SQLException, DataSetException
	{
		// Given
		when(whiteList.entrySet()).thenReturn(Collections.emptySet());

		// When
		validator.validateAllWhiteListedAuditTablesAuditAnExistingTable();
	}

	@Test
	public void testValidateAllWhiteListedAuditTablesAuditAnExistingTableWithExistingAuditedTable() throws SQLException, DataSetException
	{
		// Given
		final String auditedTable = "auditedTable";

		when(whiteList.entrySet()).thenReturn(Collections.singleton(new HashMap.SimpleEntry<>(auditedTable, auditedTable)));
		final CachedResultSetTable queryResult = mock(CachedResultSetTable.class);
		when(whiteList.entrySet()).thenReturn(Collections.emptySet());
		when(databaseQueries.getTableByName(auditedTable)).thenReturn(queryResult);
		when(queryResult.getRowCount()).thenReturn(1);

		// When
		validator.validateAllWhiteListedAuditTablesAuditAnExistingTable();
	}

	@Test
	public void testValidateAllWhiteListedAuditTablesAuditAnExistingTableWithOneAuditedTableNotExisting() throws SQLException, DataSetException
	{
		// Given
		final String auditedTable = "auditedTable";
		final String notAuditedTable = "unaudited";
		final Set<Map.Entry<String, String>> entrySet = new HashSet<>();
		entrySet.add(new HashMap.SimpleEntry<>(auditedTable, auditedTable));
		entrySet.add(new HashMap.SimpleEntry<>(notAuditedTable, notAuditedTable));

		when(whiteList.entrySet()).thenReturn(entrySet);

		final CachedResultSetTable queryResultAuditedTable = mock(CachedResultSetTable.class);
		when(databaseQueries.getTableByName(auditedTable)).thenReturn(queryResultAuditedTable);
		when(queryResultAuditedTable.getRowCount()).thenReturn(1);

		final CachedResultSetTable queryResultNotAuditedTable = mock(CachedResultSetTable.class);
		when(databaseQueries.getTableByName(notAuditedTable)).thenReturn(queryResultNotAuditedTable);

		try
		{
			// When
			validator.validateAllWhiteListedAuditTablesAuditAnExistingTable();
		}
		catch (ValidationException e)
		{
			// Then
			assertEquals("The following audit tables do not audit another table in the database, or do not have the correct mapping to the audited table: [unaudited]", e.getMessage());
		}
	}
}