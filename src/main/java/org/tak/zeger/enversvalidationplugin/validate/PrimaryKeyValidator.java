package org.tak.zeger.enversvalidationplugin.validate;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import org.dbunit.dataset.DataSetException;
import org.tak.zeger.enversvalidationplugin.annotation.ConnectionProvider;
import org.tak.zeger.enversvalidationplugin.annotation.Parameterized;
import org.tak.zeger.enversvalidationplugin.annotation.TargetPhase;
import org.tak.zeger.enversvalidationplugin.annotation.Validate;
import org.tak.zeger.enversvalidationplugin.annotation.ValidationType;
import org.tak.zeger.enversvalidationplugin.annotation.WhiteList;
import org.tak.zeger.enversvalidationplugin.connection.ConnectionProviderInstance;
import org.tak.zeger.enversvalidationplugin.exceptions.ValidationException;

@ValidationType(TargetPhase.CONSTRAINTS)
public class PrimaryKeyValidator
{
	@ConnectionProvider
	private ConnectionProviderInstance connectionProvider;

	private final String auditTableName;
	private final String auditedTableName;
	private final List<String> primaryIdentifierColumnNamesAuditTable;
	private final List<String> primaryIdentifierColumnNamesAuditedTable;

	public PrimaryKeyValidator(@Nonnull String auditTableName, @Nonnull String auditedTableName, @Nonnull List<String> primaryIdentifierColumnNamesAuditTable, @Nonnull List<String> primaryIdentifierColumnNamesAuditedTable)
	{
		this.auditTableName = auditTableName;
		this.auditedTableName = auditedTableName;
		this.primaryIdentifierColumnNamesAuditTable = primaryIdentifierColumnNamesAuditTable;
		this.primaryIdentifierColumnNamesAuditedTable = primaryIdentifierColumnNamesAuditedTable;
	}

	@Parameterized(name = "{index}: auditTableName: {0}", uniqueIdentifier = "{0}")
	public static List<Object[]> generateTestData(@Nonnull @ConnectionProvider ConnectionProviderInstance connectionProvider, @Nonnull @WhiteList Map<String, String> whiteList) throws SQLException, DataSetException
	{
		final List<Object[]> testData = new ArrayList<>();
		for (Map.Entry<String, String> whiteListEntry : whiteList.entrySet())
		{
			final List<String> primaryIdentifierColumnNamesAuditTable = connectionProvider.getQueries().getPrimaryKeyColumnNames(whiteListEntry.getKey());
			final List<String> primaryIdentifierColumnNamesAuditedTable = connectionProvider.getQueries().getPrimaryKeyColumnNames(whiteListEntry.getValue());

			testData.add(new Object[] { whiteListEntry.getKey(), whiteListEntry.getValue(), primaryIdentifierColumnNamesAuditTable, primaryIdentifierColumnNamesAuditedTable, });
		}

		return testData;
	}

	@Validate
	public void testAllAuditTablesHaveAValidPrimaryKey()
	{
		if (primaryIdentifierColumnNamesAuditTable.isEmpty())
		{
			throw new ValidationException("Audit table " + auditTableName + " has no primary key.");
		}

		final Set<String> expectedAuditTablePrimaryKeyColumnNames = new HashSet<>(primaryIdentifierColumnNamesAuditedTable);
		final String revisionTableIdentifierColumnName = connectionProvider.getQueries().getRevisionTableIdentifierColumnName();
		expectedAuditTablePrimaryKeyColumnNames.add(revisionTableIdentifierColumnName);

		expectedAuditTablePrimaryKeyColumnNames.removeAll(primaryIdentifierColumnNamesAuditTable);

		if (!expectedAuditTablePrimaryKeyColumnNames.isEmpty())
		{
			throw new ValidationException(
					//@formatter:off
					"Audit table " + auditTableName + " has a primary key that is not compromised of the primary key columns of "
					+ auditedTableName + " + " + revisionTableIdentifierColumnName +
					" the following columns are missing: " + expectedAuditTablePrimaryKeyColumnNames
					//@formatter:on
			);
		}
	}
}