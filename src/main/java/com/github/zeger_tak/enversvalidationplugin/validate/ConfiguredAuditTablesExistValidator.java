package com.github.zeger_tak.enversvalidationplugin.validate;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import com.github.zeger_tak.enversvalidationplugin.annotation.AuditTableInformationMap;
import com.github.zeger_tak.enversvalidationplugin.annotation.ConnectionProvider;
import com.github.zeger_tak.enversvalidationplugin.annotation.Parameterized;
import com.github.zeger_tak.enversvalidationplugin.annotation.TargetPhase;
import com.github.zeger_tak.enversvalidationplugin.annotation.Validate;
import com.github.zeger_tak.enversvalidationplugin.annotation.ValidationType;
import com.github.zeger_tak.enversvalidationplugin.connection.ConnectionProviderInstance;
import com.github.zeger_tak.enversvalidationplugin.entities.AuditTableInformation;
import com.github.zeger_tak.enversvalidationplugin.exceptions.SetupValidationForSpecificAuditTableInformationException;
import org.dbunit.database.CachedResultSetTable;
import org.dbunit.dataset.DataSetException;

@ValidationType(TargetPhase.SETUP)
public class ConfiguredAuditTablesExistValidator
{
	private final AuditTableInformation auditTableInformation;
	private ConnectionProviderInstance connectionProvider;

	public ConfiguredAuditTablesExistValidator(@Nonnull ConnectionProviderInstance connectionProvider, @Nonnull String auditTableName, @Nonnull AuditTableInformation auditTableInformation)
	{
		this.connectionProvider = connectionProvider;
		this.auditTableInformation = auditTableInformation;
	}

	@Parameterized(name = "{index}: auditTableName: {1}", uniqueIdentifier = "{1}")
	public static List<Object[]> generateTestData(@Nonnull @ConnectionProvider ConnectionProviderInstance connectionProvider, @Nonnull @AuditTableInformationMap Map<String, AuditTableInformation> auditTableInformationMap)
	{
		final List<Object[]> testData = new ArrayList<>();
		for (Map.Entry<String, AuditTableInformation> auditTableInformation : auditTableInformationMap.entrySet())
		{
			testData.add(new Object[] { connectionProvider, auditTableInformation.getKey(), auditTableInformation.getValue() });
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
			determineIfTablesExist(auditTableInformation);
		}
		catch (TableDoesNotExistException e)
		{
			throw new SetupValidationForSpecificAuditTableInformationException(auditTableInformation + " is not a valid " + AuditTableInformation.class.getSimpleName() + " because the table " + e.tableName + " does not exist.", auditTableInformation);
		}
	}

	private void determineIfTablesExist(@Nonnull AuditTableInformation auditTableInformation) throws SQLException, DataSetException
	{
		assertTableExists(auditTableInformation.getAuditTableName());
		assertTableExists(auditTableInformation.getContentTableName());

		final AuditTableInformation parentAuditTable = auditTableInformation.getAuditTableParent();
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