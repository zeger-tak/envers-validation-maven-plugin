package org.tak.zeger.enversvalidationplugin.validate;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.tak.zeger.enversvalidationplugin.connection.ConnectionProviderInstance;
import org.tak.zeger.enversvalidationplugin.connection.DatabaseQueries;
import org.tak.zeger.enversvalidationplugin.entities.WhitelistEntry;
import org.tak.zeger.enversvalidationplugin.exceptions.ValidationException;

public class AuditTableWhiteListValidatorTest
{
	@Rule
	public MockitoRule mockitoRule = MockitoJUnit.rule();

	@InjectMocks
	private AuditTableWhiteListValidator validator;

	@Mock
	private Map<String, WhitelistEntry> whiteList;

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
}