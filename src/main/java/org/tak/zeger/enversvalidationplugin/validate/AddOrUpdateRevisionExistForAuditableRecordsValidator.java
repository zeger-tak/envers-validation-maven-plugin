package org.tak.zeger.enversvalidationplugin.validate;

import java.math.BigDecimal;
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
import org.tak.zeger.enversvalidationplugin.entities.RevisionConstants;
import org.tak.zeger.enversvalidationplugin.entities.TableRow;
import org.tak.zeger.enversvalidationplugin.exceptions.ValidationException;
import org.tak.zeger.enversvalidationplugin.utils.DatabaseUtils;

@ValidationType(TargetPhase.CONTENT)
public class AddOrUpdateRevisionExistForAuditableRecordsValidator extends AbstractValidator
{
	private final String auditedTableName;
	private final Map<String, TableRow> recordsInAuditedTableIdentifiedByPK;
	private final Map<String, List<TableRow>> recordsInAuditTable;

	public AddOrUpdateRevisionExistForAuditableRecordsValidator(@Nonnull String auditedTableName, @Nonnull Map<String, TableRow> recordsInAuditedTableIdentifiedByPK, @Nonnull Map<String, List<TableRow>> recordsInAuditTable)
	{
		this.auditedTableName = auditedTableName;
		this.recordsInAuditedTableIdentifiedByPK = recordsInAuditedTableIdentifiedByPK;
		this.recordsInAuditTable = recordsInAuditTable;
	}

	@Parameterized(name = "{index}: auditTableName: {0}")
	public static List<Object[]> generateTestData(@Nonnull @ConnectionProvider ConnectionProviderInstance connectionProvider, @Nonnull @WhiteList Map<String, String> whiteList) throws SQLException, DataSetException
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

	@Validate
	public void testAllRecordsInAuditedTableHaveARevision()
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
			final Object columnValue = lastRecord.getColumnValue(RevisionConstants.REVTYPE_COLUMN_NAME);
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
}