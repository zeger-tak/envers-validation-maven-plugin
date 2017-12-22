package org.tak.zeger.enversvalidationplugin.validate;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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
import org.tak.zeger.enversvalidationplugin.entities.AuditTableInformation;
import org.tak.zeger.enversvalidationplugin.exceptions.ValidationException;

public class AuditTableAuditTableInformationValidatorTest
{
	@Rule
	public MockitoRule mockitoRule = MockitoJUnit.rule();

	@InjectMocks
	private AuditTableInformationMapValidator validator;

	@Mock
	private Map<String, AuditTableInformation> auditTableInformationMap;

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
	public void testGenerateData() throws SQLException, DataSetException
	{
		// Given
		final String postfix = "_aud";
		final Set<String> expectedTableNames = Collections.emptySet();
		when(databaseQueries.getAuditTablePostFix()).thenReturn(postfix);
		when(databaseQueries.getTablesByNameEndingWith(postfix)).thenReturn(expectedTableNames);

		// When
		final List<Object[]> generateData = AuditTableInformationMapValidator.generateData(connectionProvider, auditTableInformationMap);

		// Then
		assertEquals(1, generateData.size());
		assertEquals(auditTableInformationMap, generateData.get(0)[0]);
		assertEquals(expectedTableNames, generateData.get(0)[1]);
	}

	@Test
	public void testValidateAllExistingAuditTablesAreSpecifiedNoExistingTables()
	{
		// Given
		final Set<String> existingTables = Collections.emptySet();
		final String specifiedTable = "specified";

		when(auditTablesInDatabase.stream()).thenReturn(existingTables.stream());
		when(auditTableInformationMap.keySet()).thenReturn(Collections.singleton(specifiedTable));

		// When
		validator.validateAllExistingAuditTablesAreSpecified();
	}

	@Test
	public void testValidateAllExistingAuditTablesAreSpecifiedAndExist()
	{
		// Given
		final String specifiedTable = "specified";
		final Set<String> existingTables = Collections.singleton(specifiedTable);

		when(auditTablesInDatabase.stream()).thenReturn(existingTables.stream());
		when(auditTableInformationMap.keySet()).thenReturn(Collections.singleton(specifiedTable.toUpperCase()));

		// When
		validator.validateAllExistingAuditTablesAreSpecified();
	}

	@Test
	public void testValidateAllExistingAuditTablesAreSpecifiedWhenSomeExistingTablesAreNotSpecified()
	{
		// Given
		final String specifiedTable = "specified";
		final String existingTableNotSpecified = "not specified";
		final List<String> existingTables = Arrays.asList(specifiedTable, existingTableNotSpecified);

		when(auditTablesInDatabase.stream()).thenReturn(existingTables.stream());
		when(auditTableInformationMap.keySet()).thenReturn(Collections.singleton(specifiedTable.toUpperCase()));

		try
		{
			// When
			validator.validateAllExistingAuditTablesAreSpecified();
		}
		catch (ValidationException e)
		{
			assertEquals("The following audit tables are not configured in the audit table information map: [NOT SPECIFIED]", e.getMessage());
		}
	}
}