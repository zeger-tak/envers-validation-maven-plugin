package com.github.zeger_tak.enversvalidationplugin.connection;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import com.github.zeger_tak.enversvalidationplugin.entities.AuditTableInformation;
import com.github.zeger_tak.enversvalidationplugin.entities.TableRow;
import org.apache.commons.lang3.StringUtils;
import org.dbunit.database.CachedResultSetTable;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITableMetaData;

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
	public Map<String, TableRow> getContentRecords(@Nonnull IDatabaseConnection databaseConnection, @Nonnull AuditTableInformation auditTableInformation, @Nonnull List<String> primaryIdentifierColumnNames) throws SQLException, DataSetException
	{
		final CachedResultSetTable recordsInContentTable = selectAllRecordsFromTable(databaseConnection, auditTableInformation, primaryIdentifierColumnNames);
		final List<String> columnNames = getColumnNames(recordsInContentTable);

		final Map<String, TableRow> recordsInTableById = new HashMap<>();
		for (int rowIndex = 0; rowIndex < recordsInContentTable.getRowCount(); rowIndex++)
		{
			final TableRow tableRow = new TableRow();
			for (String columnName : columnNames)
			{
				tableRow.addColumn(columnName, recordsInContentTable.getValue(rowIndex, columnName));
			}

			final String identifier = getPrimaryIdentifierAsString(recordsInContentTable, rowIndex, primaryIdentifierColumnNames);
			recordsInTableById.put(identifier, tableRow);
		}

		return recordsInTableById;
	}

	@Nonnull
	private CachedResultSetTable selectAllRecordsFromTable(@Nonnull IDatabaseConnection databaseConnection, @Nonnull AuditTableInformation auditTableInformation, List<String> primaryIdentifierColumnNames) throws SQLException, DataSetException
	{
		final String query = createContentTableSelectQuery(auditTableInformation, primaryIdentifierColumnNames);
		return (CachedResultSetTable) databaseConnection.createQueryTable(auditTableInformation.getContentTableName(), query);
	}

	@Nonnull
	private String createContentTableSelectQuery(@Nonnull AuditTableInformation auditTableInformation, @Nonnull List<String> primaryIdentifierColumnNames)
	{
		final StringBuilder query = new StringBuilder("select * from ");
		query.append(auditTableInformation.getContentTableName());
		query.append(" ");
		query.append(auditTableInformation.getContentTableName());
		query.append(" ");

		final AuditTableInformation auditTableParent = auditTableInformation.getAuditTableParent();
		if (auditTableParent != null)
		{
			appendQueryWithJoinsOnParentContentTables(query, auditTableParent, auditTableInformation.getContentTableName(), primaryIdentifierColumnNames);
		}
		return query.toString();
	}

	private void appendQueryWithJoinsOnParentContentTables(@Nonnull StringBuilder query, @Nonnull AuditTableInformation auditTableInformation, @Nonnull String childAlias, @Nonnull List<String> primaryIdentifierColumnNames)
	{
		query.append("inner join ");
		query.append(auditTableInformation.getContentTableName());
		query.append(" ");
		query.append(auditTableInformation.getContentTableName());
		query.append(" on ");
		for (int i = 0; i < primaryIdentifierColumnNames.size(); i++)
		{
			final String primaryIdentifierColumnName = primaryIdentifierColumnNames.get(i);
			if (i > 0)
			{
				query.append(" and ");
			}

			query.append(auditTableInformation.getContentTableName());
			query.append(".");
			query.append(primaryIdentifierColumnName);
			query.append(" = ");
			query.append(childAlias);
			query.append(".");
			query.append(primaryIdentifierColumnName);
			query.append(" ");
		}
		final AuditTableInformation auditTableParent = auditTableInformation.getAuditTableParent();
		if (auditTableParent != null)
		{
			appendQueryWithJoinsOnParentContentTables(query, auditTableParent, auditTableInformation.getContentTableName(), primaryIdentifierColumnNames);
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
	public Map<String, List<TableRow>> getAuditRecordsGroupedByContentPrimaryKey(@Nonnull IDatabaseConnection databaseConnection, @Nonnull AuditTableInformation auditTableInformation, @Nonnull List<String> primaryIdentifierColumnNames) throws SQLException, DataSetException
	{
		final CachedResultSetTable recordsInTable = selectAllRecordsFromTableOrderByRevAscending(databaseConnection, auditTableInformation, primaryIdentifierColumnNames);
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
	private CachedResultSetTable selectAllRecordsFromTableOrderByRevAscending(@Nonnull IDatabaseConnection databaseConnection, @Nonnull AuditTableInformation auditTableInformationMap, @Nonnull List<String> primaryIdentifierColumnNames) throws SQLException, DataSetException
	{
		final String query = createAuditTableSelectQuery(auditTableInformationMap, primaryIdentifierColumnNames);
		return (CachedResultSetTable) databaseConnection.createQueryTable(auditTableInformationMap.getAuditTableName(), query);
	}

	@Nonnull
	private String createAuditTableSelectQuery(@Nonnull AuditTableInformation auditTableInformation, @Nonnull List<String> primaryIdentifierColumnNames)
	{
		final String revisionTableIdentifierColumnName = getRevisionTableIdentifierColumnName();
		final List<String> primaryIdentifierColumnsAuditTable = new ArrayList<>(primaryIdentifierColumnNames);
		primaryIdentifierColumnsAuditTable.add(revisionTableIdentifierColumnName);

		final StringBuilder query = new StringBuilder("select * from ");
		query.append(auditTableInformation.getAuditTableName());
		query.append(" ");
		query.append(auditTableInformation.getAuditTableName());
		query.append(" ");

		final AuditTableInformation auditTableParent = auditTableInformation.getAuditTableParent();
		if (auditTableParent != null)
		{
			appendQueryWithJoinsOnParentAuditTables(query, auditTableParent, auditTableInformation.getAuditTableName(), primaryIdentifierColumnsAuditTable);
		}

		query.append(" order by ");
		query.append(auditTableInformation.getAuditTableName());
		query.append(".");
		query.append(revisionTableIdentifierColumnName);

		return query.toString();
	}

	private void appendQueryWithJoinsOnParentAuditTables(@Nonnull StringBuilder query, @Nonnull AuditTableInformation auditTableInformation, @Nonnull String childAlias, @Nonnull List<String> primaryIdentifierColumnNames)
	{
		query.append("inner join ");
		query.append(auditTableInformation.getAuditTableName());
		query.append(" ");
		query.append(auditTableInformation.getAuditTableName());
		query.append(" on ");
		for (int i = 0; i < primaryIdentifierColumnNames.size(); i++)
		{
			final String primaryIdentifierColumnName = primaryIdentifierColumnNames.get(i);
			if (i > 0)
			{
				query.append(" and ");
			}

			query.append(auditTableInformation.getAuditTableName());
			query.append(".");
			query.append(primaryIdentifierColumnName);
			query.append(" = ");
			query.append(childAlias);
			query.append(".");
			query.append(primaryIdentifierColumnName);
			query.append(" ");
		}
		final AuditTableInformation auditTableParent = auditTableInformation.getAuditTableParent();
		if (auditTableParent != null)
		{
			appendQueryWithJoinsOnParentAuditTables(query, auditTableParent, auditTableInformation.getAuditTableName(), primaryIdentifierColumnNames);
		}
	}

	@Nonnull
	@Override
	public String getPrimaryIdentifierAsString(@Nonnull CachedResultSetTable recordsInContentTable, int rowIndex, @Nonnull List<String> primaryIdentifierColumnNames) throws DataSetException
	{
		List<String> primaryIdentifierValue = new ArrayList<>(primaryIdentifierColumnNames.size());
		for (String primaryIdentifierColumnName : primaryIdentifierColumnNames)
		{
			final Object identifierValue = recordsInContentTable.getValue(rowIndex, StringUtils.upperCase(primaryIdentifierColumnName));
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