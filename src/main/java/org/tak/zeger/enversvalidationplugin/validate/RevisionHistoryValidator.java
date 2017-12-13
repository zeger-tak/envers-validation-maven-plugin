package org.tak.zeger.enversvalidationplugin.validate;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import org.dbunit.dataset.DataSetException;
import org.tak.zeger.enversvalidationplugin.annotation.ConnectionProvider;
import org.tak.zeger.enversvalidationplugin.annotation.Parameterized;
import org.tak.zeger.enversvalidationplugin.annotation.TargetPhase;
import org.tak.zeger.enversvalidationplugin.annotation.Validate;
import org.tak.zeger.enversvalidationplugin.annotation.ValidationType;
import org.tak.zeger.enversvalidationplugin.annotation.WhiteList;
import org.tak.zeger.enversvalidationplugin.connection.ConnectionProviderInstance;
import org.tak.zeger.enversvalidationplugin.connection.DatabaseQueries;
import org.tak.zeger.enversvalidationplugin.entities.RevisionConstants;
import org.tak.zeger.enversvalidationplugin.entities.TableRow;
import org.tak.zeger.enversvalidationplugin.exceptions.ValidationException;

/**
 * The goal of this validator is described in its {@link Validate} methods:
 */
@ValidationType(TargetPhase.CONTENT)
public class RevisionHistoryValidator
{
	private final ConnectionProviderInstance connectionProvider;
	private final String auditedTableName;
	private final Map<String, List<TableRow>> recordsInAuditTable;
	private final Map<String, TableRow> recordsInAuditedTableIdentifiedByPK;

	public RevisionHistoryValidator(@Nonnull ConnectionProviderInstance connectionProvider, @Nonnull String auditedTableName, @Nonnull Map<String, List<TableRow>> recordsInAuditTable, @Nonnull Map<String, TableRow> recordsInAuditedTableIdentifiedByPK)
	{
		this.connectionProvider = connectionProvider;
		this.auditedTableName = auditedTableName;
		this.recordsInAuditTable = recordsInAuditTable;
		this.recordsInAuditedTableIdentifiedByPK = recordsInAuditedTableIdentifiedByPK;
	}

	@Parameterized(name = "{index}: auditTableName: {1}", uniqueIdentifier = "{1}")
	public static List<Object[]> generateTestData(@Nonnull @ConnectionProvider ConnectionProviderInstance connectionProvider, @Nonnull @WhiteList Map<String, String> whiteList) throws SQLException, DataSetException
	{
		final DatabaseQueries databaseQueries = connectionProvider.getQueries();
		final List<Object[]> testData = new ArrayList<>();
		for (Map.Entry<String, String> whiteListEntry : whiteList.entrySet())
		{
			final List<String> primaryIdentifierColumnNames = databaseQueries.getPrimaryKeyColumnNames(whiteListEntry.getValue());

			final Map<String, TableRow> recordsInAuditedTableById = databaseQueries.getRecordInTableIdentifiedByPK(connectionProvider, whiteListEntry.getValue(), primaryIdentifierColumnNames);
			final Map<String, List<TableRow>> recordsInAuditTableGroupedById = databaseQueries.getRecordsInTableGroupedByPK(connectionProvider, whiteListEntry.getKey(), primaryIdentifierColumnNames);
			testData.add(new Object[] { connectionProvider, whiteListEntry.getValue(), recordsInAuditTableGroupedById, recordsInAuditedTableById });
		}

		return testData;
	}

	/**
	 * Validates that all history flows are valid.
	 * A valid history flow consists of the following:
	 * - Starts with an Add revision.
	 * - Followed by 0 or more Update revisions.
	 * - Ending with a Remove revision.
	 *
	 * The following cases are caught by this validator, for each specific primary key:
	 * - Starts with an Update or Remove revision.
	 * - An Add revision following after either an Add or an Update revision.
	 * - Only a Remove revision, or a Remove revision following after another Remove revision.
	 * - An Update revision following after a Remove revision.
	 */
	@Validate
	public void validateHistoryIsAValidFlow()
	{
		final List<String> identifiersWithInvalidHistory = new ArrayList<>(recordsInAuditTable.size());
		for (Map.Entry<String, List<TableRow>> auditHistoryPerIdentifier : recordsInAuditTable.entrySet())
		{
			boolean existingRecord = false;

			for (TableRow tableRow : auditHistoryPerIdentifier.getValue())
			{
				final Object columnValue = tableRow.getColumnValue(connectionProvider.getQueries().getRevTypeColumnName());
				if (columnValue == RevisionConstants.DO_NOT_VALIDATE_REVISION)
				{
					break;
				}
				final int revType = ((BigDecimal) columnValue).intValue();
				if (!existingRecord && revType != RevisionConstants.ADD_REVISION)
				{
					identifiersWithInvalidHistory.add(auditHistoryPerIdentifier.getKey());
					break;
				}

				if (existingRecord && revType == RevisionConstants.ADD_REVISION)
				{
					identifiersWithInvalidHistory.add(auditHistoryPerIdentifier.getKey());
					break;
				}

				existingRecord = !(existingRecord && revType == RevisionConstants.REMOVE_REVISION);
			}
		}

		if (!identifiersWithInvalidHistory.isEmpty())
		{
			throw new ValidationException("The following identifiers " + identifiersWithInvalidHistory + " have an invalid audit history for the table " + auditedTableName);
		}
	}

	/**
	 * Validates that the latest revision for each primary key is not an Add/Update revision if there is no corresponding record in the content table.
	 */
	@Validate
	public void validateLatestAddOrUpdateRevisionRefersToExistingContent()
	{
		final List<String> recordsWithAnAddOrUpdateLatestRevisionButNoExistingContent = new ArrayList<>(recordsInAuditTable.size());
		for (Map.Entry<String, List<TableRow>> auditHistoryPerIdentifier : recordsInAuditTable.entrySet())
		{
			final List<TableRow> historyFlow = auditHistoryPerIdentifier.getValue();
			if (historyFlow.isEmpty())
			{
				continue;
			}

			final TableRow latestRevision = historyFlow.get(historyFlow.size() - 1);

			final Object columnValue = latestRevision.getColumnValue(connectionProvider.getQueries().getRevTypeColumnName());
			if (columnValue == RevisionConstants.DO_NOT_VALIDATE_REVISION)
			{
				continue;
			}
			final int revType = ((BigDecimal) columnValue).intValue();
			if (revType == RevisionConstants.REMOVE_REVISION)
			{
				continue;
			}

			final TableRow contentTableRow = recordsInAuditedTableIdentifiedByPK.get(auditHistoryPerIdentifier.getKey());
			if (contentTableRow == null)
			{
				recordsWithAnAddOrUpdateLatestRevisionButNoExistingContent.add(auditHistoryPerIdentifier.getKey());
			}
		}

		if (!recordsWithAnAddOrUpdateLatestRevisionButNoExistingContent.isEmpty())
		{
			throw new ValidationException("The following identifiers " + recordsWithAnAddOrUpdateLatestRevisionButNoExistingContent + " have a latest revision of type Add/Update but have no record present in content table " + auditedTableName + ".");
		}
	}
}