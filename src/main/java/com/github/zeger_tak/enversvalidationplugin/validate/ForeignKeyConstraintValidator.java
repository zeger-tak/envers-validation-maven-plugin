package com.github.zeger_tak.enversvalidationplugin.validate;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.github.zeger_tak.enversvalidationplugin.annotation.AuditTableInformationMap;
import com.github.zeger_tak.enversvalidationplugin.annotation.ConnectionProvider;
import com.github.zeger_tak.enversvalidationplugin.annotation.TargetPhase;
import com.github.zeger_tak.enversvalidationplugin.annotation.Validate;
import com.github.zeger_tak.enversvalidationplugin.annotation.ValidationType;
import com.github.zeger_tak.enversvalidationplugin.connection.ConnectionProviderInstance;
import com.github.zeger_tak.enversvalidationplugin.entities.AuditTableInformation;
import com.github.zeger_tak.enversvalidationplugin.exceptions.ValidationException;
import org.dbunit.dataset.DataSetException;

/**
 * The goal of this validator is described in its {@link Validate} methods:
 */
@ValidationType(TargetPhase.CONSTRAINTS)
public class ForeignKeyConstraintValidator
{
	@AuditTableInformationMap
	private Map<String, AuditTableInformation> auditTableInformationMap;

	@ConnectionProvider
	private ConnectionProviderInstance connectionProvider;

	/**
	 * Validates that no table has a foreign key to the revision table except for the speified  audit tables.
	 * This validator will catch cases not caught by {@link AuditTableInformationMapValidator}.
	 */
	@Validate
	public void validateNoForeignKeysExistsForTablesNotSpecifiedOnAuditTableInformationMap() throws SQLException, DataSetException
	{
		final Set<String> auditTablesInDatabase = connectionProvider.getQueries().getListOfTablesWithForeignKeysToRevisionTable();
		final Set<String> tablesWithForeignKeyButNotInAuditTableInformationMap = new HashSet<>(auditTablesInDatabase);

		tablesWithForeignKeyButNotInAuditTableInformationMap.removeAll(auditTableInformationMap.keySet());
		if (!tablesWithForeignKeyButNotInAuditTableInformationMap.isEmpty())
		{
			throw new ValidationException("Tables found with a reference to the revision table, which are not on the white list: " + tablesWithForeignKeyButNotInAuditTableInformationMap);
		}
	}

	/**
	 * Validates that all audit tables in the audit table infomration map have a foreign key to the revision table.
	 */
	@Validate
	public void validateAllAuditTablesHaveAForeignKeyToRevisionTable() throws SQLException, DataSetException
	{
		final Set<String> auditTablesInDatabase = connectionProvider.getQueries().getListOfTablesWithForeignKeysToRevisionTable();
		final Set<String> auditTablesWithoutAForeignKey = new HashSet<>(auditTableInformationMap.keySet());
		auditTablesWithoutAForeignKey.removeAll(auditTablesInDatabase);

		if (!auditTablesWithoutAForeignKey.isEmpty())
		{
			throw new ValidationException("The following audit tables were found without a foreign key to the revision table" + auditTablesWithoutAForeignKey + ".");
		}
	}
}