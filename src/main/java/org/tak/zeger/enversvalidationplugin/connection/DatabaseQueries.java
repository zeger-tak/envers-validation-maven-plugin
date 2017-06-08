package org.tak.zeger.enversvalidationplugin.connection;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import org.dbunit.database.CachedResultSetTable;
import org.dbunit.dataset.DataSetException;
import org.tak.zeger.enversvalidationplugin.entities.TableRow;

public interface DatabaseQueries
{
	@Nonnull
	String getAuditTablePostFix();

	@Nonnull
	String getRevTypeColumnName();

	@Nonnull
	String getRevisionTableName();
	
	@Nonnull
	CachedResultSetTable getTableByName(@Nonnull String tableName) throws SQLException, DataSetException;

	@Nonnull
	Set<String> getTablesByNameEndingWith(@Nonnull String postFix) throws SQLException, DataSetException;

	@Nonnull
	List<String> getPrimaryKeyColumnNames(@Nonnull String tableName) throws SQLException, DataSetException;

	@Nonnull
	Map<String, TableRow> getRecordInTableIdentifiedByPK(@Nonnull ConnectionProviderInstance connectionProvider, @Nonnull String tableName, @Nonnull List<String> primaryIdentifierColumnNames) throws SQLException, DataSetException;

	@Nonnull
	Map<String, List<TableRow>> getRecordsInTableGroupedByPK(@Nonnull ConnectionProviderInstance connectionProvider, @Nonnull String tableName, List<String> primaryIdentifierColumnNames) throws SQLException, DataSetException;

	@Nonnull
	Set<String> getListOfTablesWithForeignKeysToRevisionTable() throws SQLException, DataSetException;
}