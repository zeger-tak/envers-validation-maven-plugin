package org.tak.zeger.enversvalidationplugin.validate;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import org.dbunit.database.CachedResultSetTable;
import org.dbunit.dataset.DataSetException;
import org.tak.zeger.enversvalidationplugin.annotation.ConnectionProvider;
import org.tak.zeger.enversvalidationplugin.annotation.Parameterized;
import org.tak.zeger.enversvalidationplugin.annotation.TargetPhase;
import org.tak.zeger.enversvalidationplugin.annotation.Validate;
import org.tak.zeger.enversvalidationplugin.annotation.ValidationType;
import org.tak.zeger.enversvalidationplugin.annotation.WhiteList;
import org.tak.zeger.enversvalidationplugin.connection.ConnectionProviderInstance;
import org.tak.zeger.enversvalidationplugin.entities.WhitelistEntry;
import org.tak.zeger.enversvalidationplugin.exceptions.SetupValidationForSpecificWhitelistEntryException;

@ValidationType(TargetPhase.SETUP)
public class WhitelistTablesExistValidator
{
	private final WhitelistEntry whitelistEntry;
	private ConnectionProviderInstance connectionProvider;

	public WhitelistTablesExistValidator(@Nonnull ConnectionProviderInstance connectionProvider, @Nonnull String auditTableName, @Nonnull WhitelistEntry whitelistEntry)
	{
		this.connectionProvider = connectionProvider;
		this.whitelistEntry = whitelistEntry;
	}

	@Parameterized(name = "{index}: auditTableName: {1}", uniqueIdentifier = "{1}")
	public static List<Object[]> generateTestData(@Nonnull @ConnectionProvider ConnectionProviderInstance connectionProvider, @Nonnull @WhiteList Map<String, WhitelistEntry> whiteList)
	{
		final List<Object[]> testData = new ArrayList<>();
		for (Map.Entry<String, WhitelistEntry> whiteListEntry : whiteList.entrySet())
		{
			testData.add(new Object[] { connectionProvider, whiteListEntry.getKey(), whiteListEntry.getValue() });
		}

		return testData;
	}

	/**
	 * Validates all audit tables exist and audit an existing content table.
	 * If the audit table has a parent table, that table will also be validated.
	 */
	@Validate
	public void validateAuditTableAndContentTableExist() throws SQLException, DataSetException
	{
		try
		{
			determineIfTablesExist(whitelistEntry);
		}
		catch (TableDoesNotExistException e)
		{
			throw new SetupValidationForSpecificWhitelistEntryException(whitelistEntry + " is not valid for whitelist because the following table does not exist " + e.tableName + ".", whitelistEntry);
		}
	}

	private void determineIfTablesExist(@Nonnull WhitelistEntry whitelistEntry) throws SQLException, DataSetException
	{
		assertTableExists(whitelistEntry.getAuditTableName());
		assertTableExists(whitelistEntry.getContentTableName());

		final WhitelistEntry parentAuditTable = whitelistEntry.getAuditTableParent();
		if (parentAuditTable != null)
		{
			determineIfTablesExist(parentAuditTable);
		}
	}

	private void assertTableExists(@Nonnull String tableName) throws SQLException, DataSetException
	{
		final CachedResultSetTable auditTable = connectionProvider.getQueries().getTableByName(tableName);
		if (auditTable.getRowCount() != 1)
		{
			throw new TableDoesNotExistException(tableName);
		}
	}

	private final class TableDoesNotExistException extends RuntimeException
	{
		private final String tableName;

		private TableDoesNotExistException(@Nonnull String tableName)
		{
			this.tableName = tableName;
		}
	}
}