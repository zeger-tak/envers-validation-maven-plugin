package com.github.zeger_tak.enversvalidationplugin.connection;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.github.zeger_tak.enversvalidationplugin.entities.AuditTableInformation;
import com.github.zeger_tak.enversvalidationplugin.entities.TableRow;
import org.dbunit.database.CachedResultSetTable;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.DataSetException;

public interface DatabaseQueries
{
	@Nonnull
	String getAuditTablePostFix();

	@Nonnull
	String getRevTypeColumnName();

	@Nonnull
	String getRevisionTableIdentifierColumnName();

	@Nonnull
	String getRevisionTableName();

	@Nonnull
	CachedResultSetTable getTableByName(@Nonnull String tableName) throws SQLException, DataSetException;

	@Nonnull
	Set<String> getTablesByNameEndingWith(@Nonnull String postFix) throws SQLException, DataSetException;

	@Nonnull
	List<String> getPrimaryKeyColumnNames(@Nonnull String tableName) throws SQLException, DataSetException;

	@Nonnull
	Map<String, TableRow> getContentRecords(@Nonnull IDatabaseConnection databaseConnection, @Nonnull AuditTableInformation auditTableInformation, @Nonnull List<String> primaryIdentifierColumnNames) throws SQLException, DataSetException;

	@Nonnull
	Map<String, List<TableRow>> getAuditRecordsGroupedByContentPrimaryKey(@Nonnull IDatabaseConnection databaseConnection, @Nonnull AuditTableInformation auditTableInformation, List<String> primaryIdentifierColumnNames) throws SQLException, DataSetException;

	@Nonnull
	Set<String> getListOfTablesWithForeignKeysToRevisionTable() throws SQLException, DataSetException;

	@Nonnull
	Set<String> getAllColumns(@Nonnull String tableName) throws SQLException, DataSetException;

	@Nonnull
	Set<String> getAllNonnullColumns(@Nonnull String tableName) throws SQLException, DataSetException;

	@Nonnull
	String getPrimaryIdentifierAsString(@Nonnull CachedResultSetTable recordsInContentTable, int rowIndex, @Nonnull List<String> primaryIdentifierColumnNames) throws DataSetException;
}