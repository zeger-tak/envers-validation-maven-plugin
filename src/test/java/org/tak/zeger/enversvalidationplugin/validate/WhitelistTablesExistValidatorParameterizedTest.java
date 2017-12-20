package org.tak.zeger.enversvalidationplugin.validate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.dbunit.database.CachedResultSetTable;
import org.dbunit.dataset.DataSetException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.tak.zeger.enversvalidationplugin.connection.ConnectionProviderInstance;
import org.tak.zeger.enversvalidationplugin.connection.DatabaseQueries;
import org.tak.zeger.enversvalidationplugin.entities.WhitelistEntry;
import org.tak.zeger.enversvalidationplugin.exceptions.ValidationException;

@RunWith(Parameterized.class)
public class WhitelistTablesExistValidatorParameterizedTest
{
	private final ConnectionProviderInstance connectionProvider;
	private final WhitelistEntry whitelistEntry;
	private final WhitelistTablesExistValidator validator;
	private final DatabaseQueries queries;
	private final String expectedExceptionMessage;

	public WhitelistTablesExistValidatorParameterizedTest(@Nonnull String testName, @Nonnull WhitelistEntry whitelistEntry, @Nonnull List<String> allTableNames, @Nonnull List<String> tablesThatDoNotExist, @Nullable String expectedExceptionMessage) throws SQLException, DataSetException
	{
		connectionProvider = mock(ConnectionProviderInstance.class);
		queries = mock(DatabaseQueries.class);
		validator = new WhitelistTablesExistValidator(connectionProvider, whitelistEntry.getAuditTableName(), whitelistEntry);

		this.expectedExceptionMessage = expectedExceptionMessage;
		this.whitelistEntry = whitelistEntry;

		for (String tableName : allTableNames)
		{
			prepareMockedResults(tableName, tablesThatDoNotExist.contains(tableName) ? 0 : 1);
		}
	}

	@Parameterized.Parameters(name = "{0}")
	public static Collection<Object[]> data()
	{
		final String auditTable1 = "audit1";
		final String auditTable2 = "audit2";
		final String auditTable3 = "audit3";

		final String contentTable1 = "content1";
		final String contentTable2 = "content2";
		final String contentTable3 = "content3";
		final List<String> allTableNames = Arrays.asList(auditTable1, auditTable2, auditTable3, contentTable1, contentTable2, contentTable3);

		final WhitelistEntry highestLevelEntry = new WhitelistEntry(auditTable1, contentTable1);

		final WhitelistEntry midLevelEntry = new WhitelistEntry("audit2", "content2");
		midLevelEntry.setAuditTableParent(highestLevelEntry);

		final WhitelistEntry bottomLevelEntry = new WhitelistEntry("audit3", "content3");
		bottomLevelEntry.setAuditTableParent(midLevelEntry);

		final String expectedExceptionMessageAuditTable1 = " is not a valid " + WhitelistEntry.class.getSimpleName() + " because the table " + auditTable1 + " does not exist.";
		final String expectedExceptionMessageAuditTable2 = " is not a valid " + WhitelistEntry.class.getSimpleName() + " because the table " + auditTable2 + " does not exist.";
		final String expectedExceptionMessageAuditTable3 = " is not a valid " + WhitelistEntry.class.getSimpleName() + " because the table " + auditTable3 + " does not exist.";

		final String expectedExceptionMessageContentTable1 = " is not a valid " + WhitelistEntry.class.getSimpleName() + " because the table " + contentTable1 + " does not exist.";
		final String expectedExceptionMessageContentTable2 = " is not a valid " + WhitelistEntry.class.getSimpleName() + " because the table " + contentTable2 + " does not exist.";
		final String expectedExceptionMessageContentTable3 = " is not a valid " + WhitelistEntry.class.getSimpleName() + " because the table " + contentTable3 + " does not exist.";

		return Arrays.asList(new Object[][] {
				//@formatter:off
				// Happy flow, all tables exist
				{"oneEntryAllTablesExist", highestLevelEntry, allTableNames, Collections.emptyList(), null},
				{"twoEntriesAllTablesExist", midLevelEntry, allTableNames, Collections.emptyList(), null},
				{"threeEntriesAllTablesExist", bottomLevelEntry, allTableNames, Collections.emptyList(), null},

				// Bottom entry has a non existing audit table
				{"oneEntryBottomAuditTableDoesNotExist", highestLevelEntry, allTableNames, Collections.singletonList(auditTable1), highestLevelEntry + expectedExceptionMessageAuditTable1},
				{"twoEntriesBottomAuditTableDoesNotExist", midLevelEntry, allTableNames, Collections.singletonList(auditTable2), midLevelEntry + expectedExceptionMessageAuditTable2},
				{"threeEntriesBottomAuditTableDoesNotExist", bottomLevelEntry, allTableNames, Collections.singletonList(auditTable3), bottomLevelEntry + expectedExceptionMessageAuditTable3},

				// Bottom entry has a non existing content table
				{"oneEntryBottomContentTableDoesNotExist", highestLevelEntry, allTableNames, Collections.singletonList(contentTable1), highestLevelEntry + expectedExceptionMessageContentTable1},
				{"twoEntriesBottomContentTableDoesNotExist", midLevelEntry, allTableNames, Collections.singletonList(contentTable2), midLevelEntry + expectedExceptionMessageContentTable2},
				{"threeEntriesBottomContentTableDoesNotExist", bottomLevelEntry, allTableNames, Collections.singletonList(contentTable3), bottomLevelEntry + expectedExceptionMessageContentTable3},

				// Mid entry has a non existing table
				{"threeEntriesMidAuditTableDoesNotExist", bottomLevelEntry, allTableNames, Collections.singletonList(auditTable2), bottomLevelEntry + expectedExceptionMessageAuditTable2},
				{"threeEntriesMidContentTableDoesNotExist", bottomLevelEntry, allTableNames, Collections.singletonList(contentTable2), bottomLevelEntry + expectedExceptionMessageContentTable2},

				// Top entry has a non existing audit table
				{"oneEntryTopAuditTableDoesNotExist", highestLevelEntry, allTableNames, Collections.singletonList(auditTable1), highestLevelEntry + expectedExceptionMessageAuditTable1},
				{"twoEntriesTopAuditTableDoesNotExist", midLevelEntry, allTableNames, Collections.singletonList(auditTable1), midLevelEntry + expectedExceptionMessageAuditTable1},
				{"threeEntriesTopAuditTableDoesNotExist", bottomLevelEntry, allTableNames, Collections.singletonList(auditTable1), bottomLevelEntry + expectedExceptionMessageAuditTable1},

				// Top entry has a non existing content table
				{"oneEntryTopContentTableDoesNotExist", highestLevelEntry, allTableNames, Collections.singletonList(contentTable1), highestLevelEntry + expectedExceptionMessageContentTable1},
				{"twoEntriesTopContentTableDoesNotExist", midLevelEntry, allTableNames, Collections.singletonList(contentTable1), midLevelEntry + expectedExceptionMessageContentTable1},
				{"threeEntriesTopContentTableDoesNotExist", bottomLevelEntry, allTableNames, Collections.singletonList(contentTable1), bottomLevelEntry + expectedExceptionMessageContentTable1},

				// All provided table names do not exist, this test proves that this validator will always provide the same results when provided with the same content.
				{"threeEntriesTopContentAllTablesDoNotExist", bottomLevelEntry, allTableNames, allTableNames, bottomLevelEntry + expectedExceptionMessageAuditTable3},
				//@formatter:on
		});
	}

	@Before
	public void init()
	{
		when(connectionProvider.getQueries()).thenReturn(queries);
	}

	@Test
	public void testValidateAuditTableAndContentTableExist() throws SQLException, DataSetException
	{
		if (expectedExceptionMessage == null)
		{
			// When
			validator.validateAuditTableAndContentTableExist();
		}
		else
		{
			try
			{
				validator.validateAuditTableAndContentTableExist();
				fail("Expected a " + ValidationException.class.getSimpleName());
			}
			catch (ValidationException e)
			{
				assertEquals(expectedExceptionMessage, e.getMessage());
			}
		}

		// Then
		assertTrue(true);
	}

	private void prepareMockedResults(@Nonnull String tableName, int rowCount) throws SQLException, DataSetException
	{
		final CachedResultSetTable result = mock(CachedResultSetTable.class);
		when(queries.getTableByName(tableName)).thenReturn(result);
		when(result.getRowCount()).thenReturn(rowCount);
	}
}