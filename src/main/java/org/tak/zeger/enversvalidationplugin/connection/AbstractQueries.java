package org.tak.zeger.enversvalidationplugin.connection;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.StringUtils;
import org.dbunit.database.CachedResultSetTable;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITableMetaData;
import org.tak.zeger.enversvalidationplugin.entities.TableRow;
import org.tak.zeger.enversvalidationplugin.entities.WhitelistEntry;

public abstract class AbstractQueries implements DatabaseQueries
{
	private final String auditTablePostFix = "_AUD";
	private final String revTypeColumnName = "REVTYPE";
	private final String revisionTableName = "REVINFO";
	private final String revisionTableIdentifierColumnName = "REV";

	@Nonnull
	@Override
	public String getAuditTablePostFix()
	{
		return auditTablePostFix;
	}

	@Nonnull
	@Override
	public String getRevTypeColumnName()
	{
		return revTypeColumnName;
	}

	@Nonnull
	@Override
	public String getRevisionTableName()
	{
		return revisionTableName;
	}

	@Nonnull
	@Override
	public String getRevisionTableIdentifierColumnName()
	{
		return revisionTableIdentifierColumnName; //TODO: replace with query that returns the foreign key column name that references the revision table (Database specific)
	}

	@Nonnull
	@Override
	public Map<String, TableRow> getContentRecords(@Nonnull IDatabaseConnection databaseConnection, @Nonnull WhitelistEntry whitelistEntry, @Nonnull List<String> primaryIdentifierColumnNames) throws SQLException, DataSetException
	{
		final CachedResultSetTable recordsInAuditedTable = selectAllRecordsFromTable(databaseConnection, whitelistEntry, primaryIdentifierColumnNames);
		final List<String> columnNames = getColumnNames(recordsInAuditedTable);

		final Map<String, TableRow> recordsInTableById = new HashMap<>();
		for (int rowIndex = 0; rowIndex < recordsInAuditedTable.getRowCount(); rowIndex++)
		{
			final TableRow tableRow = new TableRow();
			for (String columnName : columnNames)
			{
				tableRow.addColumn(columnName, recordsInAuditedTable.getValue(rowIndex, columnName));
			}

			final String identifier = getPrimaryIdentifierAsString(recordsInAuditedTable, rowIndex, primaryIdentifierColumnNames);
			recordsInTableById.put(identifier, tableRow);
		}

		return recordsInTableById;
	}

	@Nonnull
	private CachedResultSetTable selectAllRecordsFromTable(@Nonnull IDatabaseConnection databaseConnection, @Nonnull WhitelistEntry whitelistEntry, List<String> primaryIdentifierColumnNames) throws SQLException, DataSetException
	{
		//		final String query = "select * from " + whitelistEntry.getContentTableName();
		final String query = createContentTableSelectQuery(whitelistEntry, primaryIdentifierColumnNames);
		return (CachedResultSetTable) databaseConnection.createQueryTable(whitelistEntry.getContentTableName(), query);
	}

	@Nonnull
	private String createContentTableSelectQuery(@Nonnull WhitelistEntry whitelistEntry, @Nonnull List<String> primaryIdentifierColumnNames)
	{
		final StringBuilder query = new StringBuilder("select * from ");
		query.append(whitelistEntry.getContentTableName());
		query.append(" ");
		query.append(whitelistEntry.getContentTableName());
		query.append(" ");

		final WhitelistEntry auditTableParent = whitelistEntry.getAuditTableParent();
		if (auditTableParent != null)
		{
			appendQueryWithJoinsOnParentContentTables(query, auditTableParent, whitelistEntry.getContentTableName(), primaryIdentifierColumnNames);
		}
		return query.toString();
	}

	private void appendQueryWithJoinsOnParentContentTables(@Nonnull StringBuilder query, @Nonnull WhitelistEntry whitelistEntry, @Nonnull String childAlias, @Nonnull List<String> primaryIdentifierColumnNames)
	{
		query.append("inner join ");
		query.append(whitelistEntry.getContentTableName());
		query.append(" ");
		query.append(whitelistEntry.getContentTableName());
		query.append(" on ");
		for (int i = 0; i < primaryIdentifierColumnNames.size(); i++)
		{
			final String primaryIdentifierColumnName = primaryIdentifierColumnNames.get(i);
			if (i > 0)
			{
				query.append(" and ");
			}

			query.append(whitelistEntry.getContentTableName());
			query.append(".");
			query.append(primaryIdentifierColumnName);
			query.append(" = ");
			query.append(childAlias);
			query.append(".");
			query.append(primaryIdentifierColumnName);
			query.append(" ");
		}
		final WhitelistEntry auditTableParent = whitelistEntry.getAuditTableParent();
		if (auditTableParent != null)
		{
			appendQueryWithJoinsOnParentContentTables(query, auditTableParent, whitelistEntry.getContentTableName(), primaryIdentifierColumnNames);
		}
	}

	@Nonnull
	private List<String> getColumnNames(@Nonnull CachedResultSetTable recordsInAuditTable) throws DataSetException
	{
		final ITableMetaData tableMetaData = recordsInAuditTable.getTableMetaData();
		final Column[] columns = tableMetaData.getColumns();

		List<String> columnNames = new ArrayList<>(columns.length);
		for (Column column : columns)
		{
			columnNames.add(column.getColumnName());
		}

		return columnNames;
	}

	@Nonnull
	@Override
	public Map<String, List<TableRow>> getAuditRecordsGroupedByContentPrimaryKey(@Nonnull IDatabaseConnection databaseConnection, @Nonnull WhitelistEntry whitelistEntry, @Nonnull List<String> primaryIdentifierColumnNames) throws SQLException, DataSetException
	{
		final CachedResultSetTable recordsInTable = selectAllRecordsFromTableOrderByRevAscending(databaseConnection, whitelistEntry, primaryIdentifierColumnNames);
		final List<String> columnNames = getColumnNames(recordsInTable);

		final Map<String, List<TableRow>> recordsInTableGroupedById = new HashMap<>();
		for (int rowIndex = 0; rowIndex < recordsInTable.getRowCount(); rowIndex++)
		{
			final TableRow tableRow = new TableRow();
			for (String columnName : columnNames)
			{
				tableRow.addColumn(columnName, recordsInTable.getValue(rowIndex, columnName));
			}

			final String id = getPrimaryIdentifierAsString(recordsInTable, rowIndex, primaryIdentifierColumnNames);
			recordsInTableGroupedById.computeIfAbsent(id, k -> new ArrayList<>());

			final List<TableRow> tableRows = recordsInTableGroupedById.get(id);
			tableRows.add(tableRow);
		}

		return recordsInTableGroupedById;
	}

	@Nonnull
	private CachedResultSetTable selectAllRecordsFromTableOrderByRevAscending(@Nonnull IDatabaseConnection databaseConnection, @Nonnull WhitelistEntry whitelist, @Nonnull List<String> primaryIdentifierColumnNames) throws SQLException, DataSetException
	{
		final String query = createAuditTableSelectQuery(whitelist, primaryIdentifierColumnNames);
		return (CachedResultSetTable) databaseConnection.createQueryTable(whitelist.getAuditTableName(), query);
	}

	@Nonnull
	private String createAuditTableSelectQuery(@Nonnull WhitelistEntry whitelistEntry, @Nonnull List<String> primaryIdentifierColumnNames)
	{
		final String revisionTableIdentifierColumnName = getRevisionTableIdentifierColumnName();
		final List<String> primaryIdentifierColumnsAuditTable = new ArrayList<>(primaryIdentifierColumnNames);
		primaryIdentifierColumnsAuditTable.add(revisionTableIdentifierColumnName);

		final StringBuilder query = new StringBuilder("select * from ");
		query.append(whitelistEntry.getAuditTableName());
		query.append(" ");
		query.append(whitelistEntry.getAuditTableName());
		query.append(" ");

		final WhitelistEntry auditTableParent = whitelistEntry.getAuditTableParent();
		if (auditTableParent != null)
		{
			appendQueryWithJoinsOnParentAuditTables(query, auditTableParent, whitelistEntry.getAuditTableName(), primaryIdentifierColumnsAuditTable);
		}

		query.append(" order by ");
		query.append(whitelistEntry.getAuditTableName());
		query.append(".");
		query.append(revisionTableIdentifierColumnName);

		return query.toString();
	}

	private void appendQueryWithJoinsOnParentAuditTables(@Nonnull StringBuilder query, @Nonnull WhitelistEntry whitelistEntry, @Nonnull String childAlias, @Nonnull List<String> primaryIdentifierColumnNames)
	{
		query.append("inner join ");
		query.append(whitelistEntry.getAuditTableName());
		query.append(" ");
		query.append(whitelistEntry.getAuditTableName());
		query.append(" on ");
		for (int i = 0; i < primaryIdentifierColumnNames.size(); i++)
		{
			final String primaryIdentifierColumnName = primaryIdentifierColumnNames.get(i);
			if (i > 0)
			{
				query.append(" and ");
			}

			query.append(whitelistEntry.getAuditTableName());
			query.append(".");
			query.append(primaryIdentifierColumnName);
			query.append(" = ");
			query.append(childAlias);
			query.append(".");
			query.append(primaryIdentifierColumnName);
			query.append(" ");
		}
		final WhitelistEntry auditTableParent = whitelistEntry.getAuditTableParent();
		if (auditTableParent != null)
		{
			appendQueryWithJoinsOnParentAuditTables(query, auditTableParent, whitelistEntry.getAuditTableName(), primaryIdentifierColumnNames);
		}
	}

	@Nonnull
	private String getPrimaryIdentifierAsString(@Nonnull CachedResultSetTable recordsInAuditedTable, int rowIndex, @Nonnull List<String> primaryIdentifierColumnNames) throws DataSetException
	{
		List<String> primaryIdentifierValue = new ArrayList<>(primaryIdentifierColumnNames.size());
		for (String primaryIdentifierColumnName : primaryIdentifierColumnNames)
		{
			final Object identifierValue = recordsInAuditedTable.getValue(rowIndex, StringUtils.upperCase(primaryIdentifierColumnName));
			if (identifierValue == null)
			{
				primaryIdentifierValue.add(null);
			}
			else
			{
				primaryIdentifierValue.add(identifierValue.toString());
			}
		}
		return StringUtils.join(primaryIdentifierValue, "-");
	}
}