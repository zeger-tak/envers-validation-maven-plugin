package org.tak.zeger.enversvalidationplugin.connection;

import java.sql.SQLException;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;

import org.dbunit.database.CachedResultSetTable;
import org.dbunit.dataset.DataSetException;

public interface DatabaseQueries
{
	@Nonnull
	CachedResultSetTable getTableByName(@Nonnull String tableName) throws SQLException, DataSetException;

	@Nonnull
	Set<String> getTablesByNameEndingWith(@Nonnull String postFix) throws SQLException, DataSetException;

	@Nonnull
	List<String> getPrimaryKeyColumnNames(@Nonnull String tableName) throws SQLException, DataSetException;
}