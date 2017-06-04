package org.tak.zeger.enversvalidationplugin.validate;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.ObjectUtils;
import org.dbunit.database.CachedResultSetTable;
import org.dbunit.dataset.DataSetException;
import org.tak.zeger.enversvalidationplugin.annotation.ConnectionProvider;
import org.tak.zeger.enversvalidationplugin.annotation.Parameterized;
import org.tak.zeger.enversvalidationplugin.annotation.TargetPhase;
import org.tak.zeger.enversvalidationplugin.annotation.Validate;
import org.tak.zeger.enversvalidationplugin.annotation.ValidationType;
import org.tak.zeger.enversvalidationplugin.annotation.WhiteList;
import org.tak.zeger.enversvalidationplugin.connection.ConnectionProviderInstance;
import org.tak.zeger.enversvalidationplugin.entities.RevisionConstants;
import org.tak.zeger.enversvalidationplugin.entities.TableRow;
import org.tak.zeger.enversvalidationplugin.exceptions.ValidationException;
import org.tak.zeger.enversvalidationplugin.utils.DatabaseUtils;

@ValidationType(TargetPhase.CONTENT)
public class LatestAddOrUpdateRevisionShowsCurrentContentOfAuditableRecordValidator
{
	private final String auditedTableName;
	private final Map<String, TableRow> recordsInAuditedTableIdentifiedByPK;
	private final Map<String, List<TableRow>> recordsInAuditTable;

	public LatestAddOrUpdateRevisionShowsCurrentContentOfAuditableRecordValidator(@Nonnull String auditedTableName, @Nonnull Map<String, TableRow> recordsInAuditedTableIdentifiedByPK, @Nonnull Map<String, List<TableRow>> recordsInAuditTable)
	{
		this.auditedTableName = auditedTableName;
		this.recordsInAuditedTableIdentifiedByPK = recordsInAuditedTableIdentifiedByPK;
		this.recordsInAuditTable = recordsInAuditTable;
	}

	@Validate
	public void testAllLatestAddOrRemoveRevisionsShowCurrentStatus()
	{
		final Map<String, Map<String, TableRow>> incorrectHistory = new HashMap<>();
		for (Map.Entry<String, List<TableRow>> auditHistory : recordsInAuditTable.entrySet())
		{
			final List<TableRow> auditHistoryValue = auditHistory.getValue();
			final TableRow lastRecord = auditHistoryValue.get(auditHistoryValue.size() - 1);
			final Object columnValue = lastRecord.getColumnValue(RevisionConstants.REVTYPE_COLUMN_NAME);
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

	@Parameterized(name = "{index}: auditTableName: {0}")
	public static Collection<Object[]> generateData(@ConnectionProvider ConnectionProviderInstance connectionProvider, @WhiteList Map<String, String> whiteList) throws SQLException, DataSetException
	{
		final List<Object[]> testData = new ArrayList<>();
		for (Map.Entry<String, String> whiteListEntry : whiteList.entrySet())
		{
			final CachedResultSetTable recordsInAuditedTable = DatabaseUtils.selectAllRecordsFromTable(connectionProvider, whiteListEntry.getValue());
			final List<String> primaryIdentifierColumnNames = DatabaseUtils.getPrimaryIdentifierColumnNames(connectionProvider, whiteListEntry.getValue());

			testData.add(new Object[] { whiteListEntry.getValue(), DatabaseUtils.getRecordsInAuditedTableById(recordsInAuditedTable, primaryIdentifierColumnNames), DatabaseUtils.getRecordsInAuditTableGroupedById(connectionProvider, whiteListEntry.getKey(), primaryIdentifierColumnNames) });
		}

		return testData;
	}
}