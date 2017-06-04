package org.tak.zeger.enversvalidationplugin.utils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.StringUtils;
import org.dbunit.database.CachedResultSetTable;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITableMetaData;
import org.tak.zeger.enversvalidationplugin.connection.ConnectionProviderInstance;
import org.tak.zeger.enversvalidationplugin.entities.TableRow;
import org.tak.zeger.enversvalidationplugin.exceptions.DatabaseNotSupportedException;

public final class DatabaseUtils
{
	private DatabaseUtils()
	{
	}

	@Nonnull
	public static Map<String, List<TableRow>> getRecordsInAuditTableGroupedById(@Nonnull ConnectionProviderInstance connectionProvider, @Nonnull String tableName, List<String> primaryIdentifierColumnNames) throws SQLException, DataSetException
	{
		final CachedResultSetTable recordsInTable = selectAllRecordsFromTableOrderByRevAscending(connectionProvider, tableName);
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
			if (recordsInTableGroupedById.get(id) == null)
			{
				recordsInTableGroupedById.put(id, new ArrayList<TableRow>());
			}

			final List<TableRow> tableRows = recordsInTableGroupedById.get(id);
			tableRows.add(tableRow);
		}

		return recordsInTableGroupedById;
	}

	@Nonnull
	private static CachedResultSetTable selectAllRecordsFromTableOrderByRevAscending(@Nonnull ConnectionProviderInstance connectionProvider, @Nonnull String tableName) throws SQLException, DataSetException
	{
		final String query = "select * from " + tableName + " order by REV asc";
		return (CachedResultSetTable) connectionProvider.getDatabaseConnection().createQueryTable(tableName, query);
	}

	@Nonnull
	private static List<String> getColumnNames(@Nonnull CachedResultSetTable recordsInAuditTable) throws DataSetException
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
	private static String getPrimaryIdentifierAsString(@Nonnull CachedResultSetTable recordsInAuditedTable, int rowIndex, @Nonnull List<String> primaryIdentifierColumnNames) throws DataSetException
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

	@Nonnull
	public static CachedResultSetTable selectAllRecordsFromTable(@Nonnull ConnectionProviderInstance connectionProvider, @Nonnull String tableName) throws SQLException, DataSetException
	{
		final String query = "select * from " + tableName;
		return (CachedResultSetTable) connectionProvider.getDatabaseConnection().createQueryTable(tableName, query);
	}

	@Nonnull
	public static List<String> getPrimaryIdentifierColumnNames(@Nonnull ConnectionProviderInstance liquibaseDBUnit, @Nonnull String tableName) throws DataSetException, SQLException
	{
		final String query;
		if (liquibaseDBUnit.isOracle())
		{
			query = "SELECT column_name FROM all_cons_columns WHERE constraint_name = (" + " SELECT constraint_name FROM user_constraints" + " WHERE UPPER(table_name) = UPPER('" + tableName + "') AND CONSTRAINT_TYPE = 'P'" + ")";
		}
		else if (liquibaseDBUnit.isPostgreSQL())
		{
			query = "select kcu.column_name column_name from information_schema.table_constraints tc inner join information_schema.key_column_usage kcu on tc.constraint_name = kcu.constraint_name where tc.constraint_type= 'PRIMARY KEY'" + " and UPPER(tc.table_name) = UPPER('" + tableName + "')";
		}
		else
		{
			throw new DatabaseNotSupportedException("Database not supported.");
		}

		final CachedResultSetTable result = (CachedResultSetTable) liquibaseDBUnit.getDatabaseConnection().createQueryTable(tableName, query);

		final List<String> primaryIdentifiers = new ArrayList<>();
		for (int i = 0; i < result.getRowCount(); i++)
		{
			primaryIdentifiers.add((String) result.getValue(i, "column_name"));
		}

		return primaryIdentifiers;
	}

	@Nonnull
	public static Map<String, TableRow> getRecordsInAuditedTableById(@Nonnull CachedResultSetTable recordsInAuditedTable, @Nonnull List<String> primaryIdentifierColumnNames) throws SQLException, DataSetException
	{
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

}