package org.tak.zeger.enversvalidationplugin.connection;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;

import org.dbunit.database.CachedResultSetTable;
import org.dbunit.dataset.DataSetException;

public class OracleQueries extends AbstractQueries
{
	private final ConnectionProviderInstance connectionProvider;

	public OracleQueries(@Nonnull ConnectionProviderInstance connectionProvider)
	{
		this.connectionProvider = connectionProvider;
	}

	@Nonnull
	@Override
	public CachedResultSetTable getTableByName(@Nonnull String tableName) throws SQLException, DataSetException
	{
		final String query = "select TABLE_NAME from USER_TABLES where UPPER(TABLE_NAME) = UPPER('" + tableName + "')";
		return (CachedResultSetTable) connectionProvider.getDatabaseConnection().createQueryTable("USER_TABLES", query);
	}

	@Nonnull
	@Override
	public Set<String> getTablesByNameEndingWith(@Nonnull String postFix) throws SQLException, DataSetException
	{
		final String query = "select TABLE_NAME from USER_TABLES where TABLE_NAME like '%" + connectionProvider.getQueries().getAuditTablePostFix() + "'";
		final CachedResultSetTable allKnownTablesEndingWithPostFix = (CachedResultSetTable) connectionProvider.getDatabaseConnection().createQueryTable("USER_TABLES", query);

		final Set<String> auditTablesInDatabase = new HashSet<>(allKnownTablesEndingWithPostFix.getRowCount());
		for (int i = 0; i < allKnownTablesEndingWithPostFix.getRowCount(); i++)
		{
			auditTablesInDatabase.add((String) allKnownTablesEndingWithPostFix.getValue(i, "TABLE_NAME"));
		}

		return auditTablesInDatabase;
	}

	@Nonnull
	@Override
	public List<String> getPrimaryKeyColumnNames(@Nonnull String tableName) throws SQLException, DataSetException
	{
		final String query = "SELECT COLUMN_NAME FROM all_cons_columns WHERE constraint_name = (" + " SELECT constraint_name FROM user_constraints" + " WHERE UPPER(table_name) = UPPER('" + tableName + "') AND CONSTRAINT_TYPE = 'P'" + ")";
		final CachedResultSetTable result = (CachedResultSetTable) connectionProvider.getDatabaseConnection().createQueryTable(tableName, query);

		final List<String> primaryIdentifiers = new ArrayList<>();
		for (int i = 0; i < result.getRowCount(); i++)
		{
			primaryIdentifiers.add((String) result.getValue(i, "COLUMN_NAME"));
		}

		return primaryIdentifiers;
	}
}