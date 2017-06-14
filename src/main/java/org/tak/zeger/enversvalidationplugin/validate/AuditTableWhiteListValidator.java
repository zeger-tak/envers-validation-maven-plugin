package org.tak.zeger.enversvalidationplugin.validate;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.dbunit.database.CachedResultSetTable;
import org.dbunit.dataset.DataSetException;
import org.tak.zeger.enversvalidationplugin.annotation.ConnectionProvider;
import org.tak.zeger.enversvalidationplugin.annotation.ListOfAuditTablesInDatabase;
import org.tak.zeger.enversvalidationplugin.annotation.TargetPhase;
import org.tak.zeger.enversvalidationplugin.annotation.Validate;
import org.tak.zeger.enversvalidationplugin.annotation.ValidationType;
import org.tak.zeger.enversvalidationplugin.annotation.WhiteList;
import org.tak.zeger.enversvalidationplugin.connection.ConnectionProviderInstance;
import org.tak.zeger.enversvalidationplugin.exceptions.ValidationException;

@ValidationType(TargetPhase.TABLE_STRUCTURE)
public class AuditTableWhiteListValidator
{
	@WhiteList
	private Map<String, String> whiteList;

	@ListOfAuditTablesInDatabase
	private Set<String> auditTablesInDatabase;

	@ConnectionProvider
	private ConnectionProviderInstance connectionProvider;

	@Validate
	public void validateAllExistingAuditTablesAreWhiteListed()
	{
		final Set<String> whiteListedAuditTables = whiteList.keySet();

		final Set<String> auditTablesNotOnWhiteList = auditTablesInDatabase.stream().map(String::toUpperCase).collect(Collectors.toSet());
		auditTablesNotOnWhiteList.removeAll(whiteListedAuditTables);

		if (!auditTablesNotOnWhiteList.isEmpty())
		{
			throw new ValidationException("The following audit tables are not whitelisted: " + auditTablesNotOnWhiteList);
		}
	}

	@Validate
	public void validateAllWhiteListedAuditTablesExist()
	{
		final Set<String> whiteListedAuditTables = whiteList.keySet();

		final Set<String> whiteListedAuditTablesThatDoNotExistInDatabase = new HashSet<>(whiteListedAuditTables);
		whiteListedAuditTablesThatDoNotExistInDatabase.removeAll(auditTablesInDatabase.stream().map(String::toUpperCase).collect(Collectors.toSet()));

		if (!whiteListedAuditTablesThatDoNotExistInDatabase.isEmpty())
		{
			throw new ValidationException("The following whitelisted tables do not exist in the database: " + whiteListedAuditTablesThatDoNotExistInDatabase);
		}
	}

	@Validate
	public void validateAllWhiteListedAuditTablesAuditAnExistingTable() throws SQLException, DataSetException
	{
		final Set<String> auditTablesWithoutATableToAudit = new HashSet<>(whiteList.size());
		for (Map.Entry<String, String> whiteListEntry : whiteList.entrySet())
		{
			final String auditedTableName = whiteListEntry.getValue();
			final CachedResultSetTable auditTable = connectionProvider.getQueries().getTableByName(auditedTableName);
			if (auditTable.getRowCount() != 1)
			{
				final String auditTableName = whiteListEntry.getKey();
				auditTablesWithoutATableToAudit.add(auditTableName);
			}
		}

		if (!auditTablesWithoutATableToAudit.isEmpty())
		{
			throw new ValidationException("The following audit tables do not audit another table in the database, or do not have the correct mapping to the audited table: " + auditTablesWithoutATableToAudit);
		}
	}
}