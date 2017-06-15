package org.tak.zeger.enversvalidationplugin.validate;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
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
import org.tak.zeger.enversvalidationplugin.exceptions.ValidationException;

@ValidationType(TargetPhase.CONTENT)
public class RevisionValidator
{
	private final ConnectionProviderInstance connectionProvider;
	private final String auditedTableName;
	private final Map<String, TableRow> recordsInAuditedTableIdentifiedByPK;
	private final Map<String, List<TableRow>> recordsInAuditTable;

	public RevisionValidator(@Nonnull ConnectionProviderInstance connectionProvider, @Nonnull String auditedTableName, @Nonnull Map<String, TableRow> recordsInAuditedTableIdentifiedByPK, @Nonnull Map<String, List<TableRow>> recordsInAuditTable)
	{
		this.connectionProvider = connectionProvider;
		this.auditedTableName = auditedTableName;
		this.recordsInAuditedTableIdentifiedByPK = recordsInAuditedTableIdentifiedByPK;
		this.recordsInAuditTable = recordsInAuditTable;
	}

	@Parameterized(name = "{index}: auditTableName: {0}", uniqueIdentifier = "{0}")
	public static List<Object[]> generateTestData(@Nonnull @ConnectionProvider ConnectionProviderInstance connectionProvider, @Nonnull @WhiteList Map<String, String> whiteList) throws SQLException, DataSetException
	{
		final DatabaseQueries databaseQueries = connectionProvider.getQueries();
		final List<Object[]> testData = new ArrayList<>();
		for (Map.Entry<String, String> whiteListEntry : whiteList.entrySet())
		{
			final List<String> primaryIdentifierColumnNames = databaseQueries.getPrimaryKeyColumnNames(whiteListEntry.getValue());

			final Map<String, TableRow> recordsInAuditedTableById = databaseQueries.getRecordInTableIdentifiedByPK(connectionProvider, whiteListEntry.getValue(), primaryIdentifierColumnNames);
			final Map<String, List<TableRow>> recordsInAuditTableGroupedById = databaseQueries.getRecordsInTableGroupedByPK(connectionProvider, whiteListEntry.getKey(), primaryIdentifierColumnNames);
			testData.add(new Object[] { connectionProvider, whiteListEntry.getValue(), recordsInAuditedTableById, recordsInAuditTableGroupedById });
		}

		return testData;
	}

	@Validate
	public void validatetAllRecordsInAuditedTableHaveARevision()
	{
		final List<String> identifiersWhichShouldHaveAnAddOrUpdateRevision = new ArrayList<>(recordsInAuditedTableIdentifiedByPK.size());
		for (Map.Entry<String, TableRow> auditedRow : recordsInAuditedTableIdentifiedByPK.entrySet())
		{
			final List<TableRow> auditHistoryValue = recordsInAuditTable.get(auditedRow.getKey());
			if (auditHistoryValue == null)
			{
				identifiersWhichShouldHaveAnAddOrUpdateRevision.add(auditedRow.getKey());
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
				identifiersWhichShouldHaveAnAddOrUpdateRevision.add(auditedRow.getKey());
			}
		}

		if (!identifiersWhichShouldHaveAnAddOrUpdateRevision.isEmpty())
		{
			throw new ValidationException("The following identifiers " + identifiersWhichShouldHaveAnAddOrUpdateRevision + " in table " + auditedTableName + " do not have an add/update revision table as their last revision or do not have a revision at all");
		}
	}

	@Validate
	public void validateAllLatestAddOrRemoveRevisionsShowCurrentStatus()
	{
		final Map<String, Map<String, TableRow>> incorrectHistory = new HashMap<>();
		for (Map.Entry<String, List<TableRow>> auditHistory : recordsInAuditTable.entrySet())
		{
			final List<TableRow> auditHistoryValue = auditHistory.getValue();
			final TableRow lastRecord = auditHistoryValue.get(auditHistoryValue.size() - 1);
			final Object columnValue = lastRecord.getColumnValue(connectionProvider.getQueries().getRevTypeColumnName());
			if (columnValue == RevisionConstants.DO_NOT_VALIDATE_REVISION)
			{
				continue;
			}
			final int revType = ((BigDecimal) columnValue).intValue();
			if (revType != RevisionConstants.ADD_REVISION && revType != RevisionConstants.UPDATE_REVISION)
			{
				continue;
			}

			final TableRow actualRecord = recordsInAuditedTableIdentifiedByPK.get(auditHistory.getKey());
			if (actualRecord == null)
			{
				incorrectHistory.put(auditHistory.getKey(), Collections.emptyMap());
				continue;
			}

			final Set<String> columnNames = actualRecord.getColumnNames();
			final Map<String, TableRow> incorrectColumns = new HashMap<>();
			for (String columnName : columnNames)
			{
				final Object auditedValue = lastRecord.getColumnValue(columnName);
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

			if (!incorrectColumns.isEmpty())
			{
				incorrectHistory.put(auditHistory.getKey(), incorrectColumns);
			}
		}

		if (!incorrectHistory.isEmpty())
		{
			throw new ValidationException("The latest (add/update) revision for the following identifiers: " + incorrectHistory + " for table " + auditedTableName + " does not reflect the actual values in the table.");
		}
	}

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
					continue;
				}

				if (existingRecord && revType == RevisionConstants.ADD_REVISION)
				{
					identifiersWithInvalidHistory.add(auditHistoryPerIdentifier.getKey());
					continue;
				}

				existingRecord = !(existingRecord && revType == RevisionConstants.REMOVE_REVISION);
			}
		}

		if (!identifiersWithInvalidHistory.isEmpty())
		{
			throw new ValidationException("The following identifiers " + identifiersWithInvalidHistory + " have an invalid audit history for the table " + auditedTableName);
		}
	}
}