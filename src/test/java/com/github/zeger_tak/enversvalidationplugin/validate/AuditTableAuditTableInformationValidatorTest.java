package com.github.zeger_tak.enversvalidationplugin.validate;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.mockito.Mockito.when;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.github.zeger_tak.enversvalidationplugin.connection.ConnectionProviderInstance;
import com.github.zeger_tak.enversvalidationplugin.connection.DatabaseQueries;
import com.github.zeger_tak.enversvalidationplugin.entities.AuditTableInformation;
import com.github.zeger_tak.enversvalidationplugin.exceptions.ValidationException;
import org.dbunit.dataset.DataSetException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class AuditTableAuditTableInformationValidatorTest
{
	@Rule
	public MockitoRule mockitoRule = MockitoJUnit.rule();

	@InjectMocks
	private AuditTableInformationMapValidator validator;

	@Mock
	private ConnectionProviderInstance connectionProvider;

	@Mock
	private DatabaseQueries databaseQueries;

	private Map<String, AuditTableInformation> auditTableInformationMap;
	private Set<String> auditTablesInDatabase;

	@Before
	public void init()
	{
		auditTableInformationMap = Mockito.mock(Map.class);
		auditTablesInDatabase = Mockito.mock(Set.class);
		validator = new AuditTableInformationMapValidator(auditTableInformationMap, auditTablesInDatabase);
		MockitoAnnotations.initMocks(this);

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

	@Test
	public void testValidateAllContentTablesHaveAllColumnsInAuditTable_noForgottenColumns() throws DataSetException, SQLException
	{
		// Given
		final Set<String> contentTableColumns = Stream.of("AuditedCol", "UnAuditedCol").collect(Collectors.toSet());
		final Set<String> auditTableColumns = Stream.of("AuditedCol", "InAuditButNotInContent").collect(Collectors.toSet());
		final Set<String> unAuditedColumns = Stream.of("UnAuditedCol".toUpperCase()).collect(Collectors.toSet());

		final AuditTableInformation auditTableInformation = new AuditTableInformation("AuditTableName", "ContentTableName", unAuditedColumns);

		when(databaseQueries.getAllColumns(auditTableInformation.getContentTableName())).thenReturn(contentTableColumns);
		when(databaseQueries.getAllColumns(auditTableInformation.getAuditTableName())).thenReturn(auditTableColumns);

		when(auditTableInformationMap.values()).thenReturn(Arrays.asList(auditTableInformation));

		// When
		validator.validateAllContentTablesHaveAllColumnsInAuditTable();
	}

	@Test
	public void testValidateAllContentTablesHaveAllColumnsInAuditTable_withForgottenColumn() throws DataSetException, SQLException
	{
		// Given
		final Set<String> contentTableColumns = Stream.of("AuditedCol", "UnAuditedCol", "ForgottenCol").collect(Collectors.toSet());
		final Set<String> auditTableColumns = Stream.of("AuditedCol", "InAuditButNotInContent").collect(Collectors.toSet());
		final Set<String> unAuditedColumns = Stream.of("UnAuditedCol".toUpperCase()).collect(Collectors.toSet());

		final AuditTableInformation auditTableInformation = new AuditTableInformation("AuditTableName", "ContentTableName", unAuditedColumns);

		when(databaseQueries.getAllColumns(auditTableInformation.getContentTableName())).thenReturn(contentTableColumns);
		when(databaseQueries.getAllColumns(auditTableInformation.getAuditTableName())).thenReturn(auditTableColumns);

		when(auditTableInformationMap.values()).thenReturn(Arrays.asList(auditTableInformation));

		try
		{
			// When
			validator.validateAllContentTablesHaveAllColumnsInAuditTable();
			fail("Expected ValidationException");
		}
		catch (ValidationException e)
		{
			assertEquals("The following columns are missing: AuditTableName.ForgottenCol", e.getMessage());
		}
	}
}