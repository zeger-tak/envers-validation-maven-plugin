package org.tak.zeger.enversvalidationplugin.validate;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.dbunit.database.CachedResultSetTable;
import org.dbunit.dataset.DataSetException;
import org.tak.zeger.enversvalidationplugin.annotation.TargetPhase;
import org.tak.zeger.enversvalidationplugin.annotation.Validate;
import org.tak.zeger.enversvalidationplugin.annotation.ValidationType;
import org.tak.zeger.enversvalidationplugin.exceptions.ValidationException;

@ValidationType(TargetPhase.TABLE_STRUCTURE)
public class AuditTableWhiteListValidator extends AbstractValidator
{
	@Validate
	public void testAllExistingAuditTablesAreWhiteListed()
	{
		final Set<String> whiteListedAuditTables = getWhiteList().keySet();

		final Set<String> auditTablesNotOnWhiteList = determineAuditTablesNotOnWhiteList(getAuditTablesInDatabase(), whiteListedAuditTables);

		if (!auditTablesNotOnWhiteList.isEmpty())
		{
			throw new ValidationException("The following audit tables are not whitelisted: " + auditTablesNotOnWhiteList);
		}
	}

	@Validate
	public void testAllWhiteListedAuditTablesExist()
	{
		final Set<String> whiteListedAuditTables = getWhiteList().keySet();

		final Set<String> whiteListedAuditTablesThatDoNotExistInDatabase = determineWhiteListedAuditTablesThatDoNotExistInDatabase(whiteListedAuditTables, getAuditTablesInDatabase());

		if (!whiteListedAuditTablesThatDoNotExistInDatabase.isEmpty())
		{
			throw new ValidationException("The following whitelisted tables do not exist in the database: " + whiteListedAuditTablesThatDoNotExistInDatabase);
		}
	}

	@Validate
	public void testAllWhiteListedAuditTablesAuditAnExistingTable() throws SQLException, DataSetException
	{
		final Set<String> auditTablesWithoutATableToAudit = new HashSet<>(getWhiteList().size());
		for (Map.Entry<String, String> whiteListEntry : getWhiteList().entrySet())
		{
			final String auditedTableName = whiteListEntry.getValue();
			final CachedResultSetTable auditTable = getConnectionProvider().getQueries().getTableByName(auditedTableName);
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

	@Nonnull
	private Set<String> determineWhiteListedAuditTablesThatDoNotExistInDatabase(@Nonnull Set<String> whiteListedAuditTables, @Nonnull Set<String> auditTablesInDatabase)
	{
		final Set<String> whiteListedAuditTablesThatDoNotExistInDatabase = new HashSet<>(whiteListedAuditTables);
		whiteListedAuditTablesThatDoNotExistInDatabase.removeAll(auditTablesInDatabase.stream().map(String::toUpperCase).collect(Collectors.toSet()));

		return whiteListedAuditTablesThatDoNotExistInDatabase;
	}

	@Nonnull
	private Set<String> determineAuditTablesNotOnWhiteList(@Nonnull Set<String> auditTablesInDatabase, @Nonnull Set<String> whiteListedAuditTables)
	{
		final Set<String> auditTablesNotOnWhiteList = auditTablesInDatabase.stream().map(String::toUpperCase).collect(Collectors.toSet());
		auditTablesNotOnWhiteList.removeAll(whiteListedAuditTables);

		return auditTablesNotOnWhiteList;
	}
}