package org.tak.zeger.enversvalidationplugin.connection;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;

import org.dbunit.database.CachedResultSetTable;
import org.dbunit.dataset.DataSetException;

public class PostgresQueries extends AbstractQueries
{
	private final ConnectionProviderInstance connectionProvider;

	public PostgresQueries(@Nonnull ConnectionProviderInstance connectionProvider)
	{
		this.connectionProvider = connectionProvider;
	}

	@Nonnull
	@Override
	public String getRevTypeColumnName()
	{
		return super.getRevTypeColumnName().toLowerCase();
	}

	@Nonnull
	@Override
	public String getRevisionTableName()
	{
		return super.getRevisionTableName().toLowerCase();
	}
	
	@Nonnull
	@Override
	public CachedResultSetTable getTableByName(@Nonnull String tableName) throws SQLException, DataSetException
	{
		final String query = "select upper(table_name) table_name from information_schema.tables where UPPER(TABLE_NAME) = UPPER('" + tableName + "')";
		return (CachedResultSetTable) connectionProvider.getDatabaseConnection().createQueryTable("tables", query);
	}

	@Nonnull
	@Override
	public Set<String> getTablesByNameEndingWith(@Nonnull String postFix) throws SQLException, DataSetException
	{
		final String query = "select table_name from information_schema.tables where UPPER(TABLE_NAME) like '%" + connectionProvider.getQueries().getAuditTablePostFix() + "'";

		final CachedResultSetTable allKnownTablesEndingWithPostFix = (CachedResultSetTable) connectionProvider.getDatabaseConnection().createQueryTable("tables", query);
		final Set<String> auditTablesInDatabase = new HashSet<>(allKnownTablesEndingWithPostFix.getRowCount());
		for (int i = 0; i < allKnownTablesEndingWithPostFix.getRowCount(); i++)
		{
			auditTablesInDatabase.add((String) allKnownTablesEndingWithPostFix.getValue(i, "table_name"));
		}

		return auditTablesInDatabase;
	}

	@Nonnull
	@Override
	public List<String> getPrimaryKeyColumnNames(@Nonnull String tableName) throws SQLException, DataSetException
	{
		final String query = "select kcu.column_name from information_schema.table_constraints tc inner join information_schema.key_column_usage kcu on tc.constraint_name = kcu.constraint_name where tc.constraint_type= 'PRIMARY KEY'" + " and UPPER(tc.table_name) = UPPER('" + tableName + "')";
		final CachedResultSetTable result = (CachedResultSetTable) connectionProvider.getDatabaseConnection().createQueryTable(tableName, query);

		final List<String> primaryIdentifiers = new ArrayList<>();
		for (int i = 0; i < result.getRowCount(); i++)
		{
			primaryIdentifiers.add((String) result.getValue(i, "column_name"));
		}
		return primaryIdentifiers;
	}

	@Nonnull
	@Override
	public Set<String> getListOfTablesWithForeignKeysToRevisionTable() throws SQLException, DataSetException
	{
		final String query =
				//@formatter:off	
				"select upper(tc.table_name) table_name from information_schema.referential_constraints rc " 
				+ "inner join information_schema.table_constraints tc on tc.constraint_name = rc.constraint_name " 
				+ "inner join information_schema.table_constraints tc2 on tc2.constraint_name = rc.unique_constraint_name " 
				+ "where tc2.constraint_type = 'PRIMARY KEY' and tc2.table_name = '" + getRevisionTableName() + "'";
				//@formatter:on

		final CachedResultSetTable tablesInDatabaseWithForeignKeyToRevisionTable = (CachedResultSetTable) connectionProvider.getDatabaseConnection().createQueryTable("USER_TABLES", query);

		final Set<String> auditTablesInDatabase = new HashSet<>(tablesInDatabaseWithForeignKeyToRevisionTable.getRowCount());
		for (int i = 0; i < tablesInDatabaseWithForeignKeyToRevisionTable.getRowCount(); i++)
		{
			auditTablesInDatabase.add((String) tablesInDatabaseWithForeignKeyToRevisionTable.getValue(i, "TABLE_NAME"));
		}

		return auditTablesInDatabase;
	}
}