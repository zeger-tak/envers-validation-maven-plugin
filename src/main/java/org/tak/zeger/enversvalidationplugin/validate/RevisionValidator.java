package org.tak.zeger.enversvalidationplugin.validate;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.ObjectUtils;
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
import org.tak.zeger.enversvalidationplugin.entities.WhitelistEntry;
import org.tak.zeger.enversvalidationplugin.exceptions.ValidationException;

/**
 * The goal of this validator is described in its {@link Validate} methods:
 */
@ValidationType(TargetPhase.CONTENT)
public class RevisionValidator
{
	private final ConnectionProviderInstance connectionProvider;
	private final WhitelistEntry whitelistEntry;
	private final Map<String, List<TableRow>> recordsInAuditTable;
	private final Map<String, TableRow> recordsInAuditedTableIdentifiedByPK;

	public RevisionValidator(@Nonnull ConnectionProviderInstance connectionProvider, @Nonnull WhitelistEntry whitelistEntry, @Nonnull Map<String, List<TableRow>> recordsInAuditTable, @Nonnull Map<String, TableRow> recordsInAuditedTableIdentifiedByPK)
	{
		this.connectionProvider = connectionProvider;
		this.whitelistEntry = whitelistEntry;
		this.recordsInAuditTable = recordsInAuditTable;
		this.recordsInAuditedTableIdentifiedByPK = recordsInAuditedTableIdentifiedByPK;
	}

	@Parameterized(name = "{index}: auditTableName: {1}", uniqueIdentifier = "{1}")
	public static List<Object[]> generateTestData(@Nonnull @ConnectionProvider ConnectionProviderInstance connectionProvider, @Nonnull @WhiteList Map<String, WhitelistEntry> whiteList) throws SQLException, DataSetException
	{
		final DatabaseQueries databaseQueries = connectionProvider.getQueries();
		final List<Object[]> testData = new ArrayList<>();
		for (Map.Entry<String, WhitelistEntry> whiteListEntry : whiteList.entrySet())
		{
			final List<String> primaryIdentifierColumnNames = databaseQueries.getPrimaryKeyColumnNames(whiteListEntry.getValue().getContentTableName());

			final Map<String, TableRow> recordsInAuditedTableById = databaseQueries.getRecordInTableIdentifiedByPK(connectionProvider, whiteListEntry.getValue().getContentTableName(), primaryIdentifierColumnNames);
			final Map<String, List<TableRow>> recordsInAuditTableGroupedById = databaseQueries.getRecordsInTableGroupedByPK(connectionProvider, whiteListEntry.getKey(), primaryIdentifierColumnNames);
			testData.add(new Object[] { connectionProvider, whiteListEntry.getValue(), recordsInAuditTableGroupedById, recordsInAuditedTableById });
		}

		return testData;
	}

	/**
	 * Validates that all history flows are valid.
	 * A valid history flow consists of the following:
	 * - Starts with an Add revision.
	 * - Followed by 0 or more Modify revisions.
	 * - Ending with a Remove revision.
	 *
	 * The following cases are caught by this validator, for each specific primary key:
	 * - Starts with an Modify or Remove revision.
	 * - An Add revision following after either an Add or an Modify revision.
	 * - Only a Remove revision, or a Remove revision following after another Remove revision.
	 * - An Modify revision following after a Remove revision.
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
			throw new ValidationException("The following identifiers " + identifiersWithInvalidHistory + " have an invalid audit history in " + whitelistEntry.getAuditTableName() + " for the table " + whitelistEntry.getContentTableName());
		}
	}

	/**
	 * Validates that the latest revision for each primary key is not an Add/Modify revision if there is no corresponding record in the content table.
	 */
	@Validate
	public void validateLatestAddOrModifyRevisionRefersToExistingContent()
	{
		final List<String> recordsWithAnAddOrModifyLatestRevisionButNoExistingContent = new ArrayList<>(recordsInAuditTable.size());
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
				recordsWithAnAddOrModifyLatestRevisionButNoExistingContent.add(auditHistoryPerIdentifier.getKey());
			}
		}

		if (!recordsWithAnAddOrModifyLatestRevisionButNoExistingContent.isEmpty())
		{
			throw new ValidationException("The following identifiers " + recordsWithAnAddOrModifyLatestRevisionButNoExistingContent + " have a latest revision of type Add/Modify but have no record present in content table " + whitelistEntry.getContentTableName() + ".");
		}
	}

	/**
	 * Validates all records in content table have a valid latest revision, meaning:
	 * - Record is of type Add/Modify.
	 * - Record values in audit table fully match the record values in the content table.
	 * - The audit table may have columns which are not present in the content table.
	 * - The content table may not have columns which are not present in the audit table.
	 */
	@Validate
	public void validateAllRecordsInAuditedTableHaveAValidLatestRevision()
	{
		final List<String> identifiersWhichShouldHaveAnAddOrModifyRevision = new ArrayList<>(recordsInAuditedTableIdentifiedByPK.size());
		final Map<String, Map<String, TableRow>> rowsWithDifferentValues = new HashMap<>();
		for (Map.Entry<String, TableRow> auditedRow : recordsInAuditedTableIdentifiedByPK.entrySet())
		{
			final String primaryKeyIdentifier = auditedRow.getKey();
			final List<TableRow> auditHistoryValue = recordsInAuditTable.get(primaryKeyIdentifier);
			if (auditHistoryValue == null)
			{
				identifiersWhichShouldHaveAnAddOrModifyRevision.add(primaryKeyIdentifier);
				continue;
			}

			final TableRow lastRecord = auditHistoryValue.get(auditHistoryValue.size() - 1);
			final Object columnValue = lastRecord.getColumnValue(connectionProvider.getQueries().getRevTypeColumnName());
			if (columnValue == RevisionConstants.DO_NOT_VALIDATE_REVISION)
			{
				continue;
			}
			final int revType = ((BigDecimal) columnValue).intValue();
			if (revType == RevisionConstants.REMOVE_REVISION)
			{
				identifiersWhichShouldHaveAnAddOrModifyRevision.add(primaryKeyIdentifier);
				continue;
			}

			final Map<String, TableRow> incorrectColumns = determineIncorrectColumns(auditedRow.getValue(), lastRecord);
			if (!incorrectColumns.isEmpty())
			{
				rowsWithDifferentValues.put(primaryKeyIdentifier, incorrectColumns);
			}
		}

		validateLatestRevisionComparisonResult(identifiersWhichShouldHaveAnAddOrModifyRevision, rowsWithDifferentValues);
	}

	/**
	 * TODO: Include parent table column values if parent table is specified.
	 */
	@Nonnull
	Map<String, TableRow> determineIncorrectColumns(@Nonnull TableRow actualRecord, @Nonnull TableRow lastRevision)
	{
		final Set<String> columnNames = actualRecord.getColumnNames();
		final Map<String, TableRow> incorrectColumns = new HashMap<>();
		for (String columnName : columnNames)
		{
			final Object auditedValue = lastRevision.getColumnValue(columnName);
			final Object actualColumnValue = actualRecord.getColumnValue(columnName);

			final int compare = ObjectUtils.compare((Comparable) actualColumnValue, (Comparable) auditedValue);
			if (compare != 0)
			{
				if (incorrectColumns.isEmpty())
				{
					incorrectColumns.put("actual", new TableRow());
					incorrectColumns.put("audited", new TableRow());
				}

				incorrectColumns.get("actual").addColumn(columnName, actualColumnValue);
				incorrectColumns.get("audited").addColumn(columnName, auditedValue);
			}
		}
		return incorrectColumns;
	}

	void validateLatestRevisionComparisonResult(@Nonnull List<String> identifiersWhichShouldHaveAnAddOrModifyRevision, @Nonnull Map<String, Map<String, TableRow>> rowsWithDifferentValues)
	{
		final StringBuilder errorMessage = new StringBuilder();
		if (!identifiersWhichShouldHaveAnAddOrModifyRevision.isEmpty())
		{
			errorMessage.append("The following identifiers ");
			errorMessage.append(identifiersWhichShouldHaveAnAddOrModifyRevision);
			errorMessage.append(" in table ");
			errorMessage.append(whitelistEntry.getContentTableName());
			errorMessage.append(" do not have an Add/Modify revision in table ");
			errorMessage.append(whitelistEntry.getAuditTableName());
			errorMessage.append(" as their last revision or do not have a revision at all.");

			if (!rowsWithDifferentValues.isEmpty())
			{
				errorMessage.append("\n");
			}
		}

		for (Map.Entry<String, Map<String, TableRow>> identifierWithDifferentRowValues : rowsWithDifferentValues.entrySet())
		{
			errorMessage.append("Row with identifier ");
			errorMessage.append(identifierWithDifferentRowValues.getKey());
			errorMessage.append(" has a different audit row than the actual value in the table to audit, the following columns differ: \n");

			final Map<String, TableRow> records = identifierWithDifferentRowValues.getValue();
			final TableRow actualColumnValues = records.get("actual");
			final TableRow auditedColumnValues = records.get("audited");

			for (String columnName : actualColumnValues.getColumnNames())
			{
				errorMessage.append("	Actual value for column ");
				errorMessage.append(columnName);
				errorMessage.append(": ");
				errorMessage.append(actualColumnValues.getColumnValue(columnName));
				errorMessage.append(", audited value: ");
				errorMessage.append(auditedColumnValues.getColumnValue(columnName));
				errorMessage.append(".\n");
			}
		}

		if (errorMessage.length() > 0)
		{
			throw new ValidationException(errorMessage.toString());
		}
	}
}