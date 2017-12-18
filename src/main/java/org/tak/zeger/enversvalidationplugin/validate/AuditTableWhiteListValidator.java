package org.tak.zeger.enversvalidationplugin.validate;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.dbunit.dataset.DataSetException;
import org.tak.zeger.enversvalidationplugin.annotation.ConnectionProvider;
import org.tak.zeger.enversvalidationplugin.annotation.Parameterized;
import org.tak.zeger.enversvalidationplugin.annotation.TargetPhase;
import org.tak.zeger.enversvalidationplugin.annotation.Validate;
import org.tak.zeger.enversvalidationplugin.annotation.ValidationType;
import org.tak.zeger.enversvalidationplugin.annotation.WhiteList;
import org.tak.zeger.enversvalidationplugin.connection.ConnectionProviderInstance;
import org.tak.zeger.enversvalidationplugin.entities.WhitelistEntry;
import org.tak.zeger.enversvalidationplugin.exceptions.ValidationException;

/**
 * The goal of this validator is described in its {@link Validate} methods:
 */
@ValidationType(TargetPhase.TABLE_STRUCTURE)
public class AuditTableWhiteListValidator
{
	private final Map<String, WhitelistEntry> whiteList;
	private final Set<String> auditTablesInDatabase;

	public AuditTableWhiteListValidator(@Nonnull Map<String, WhitelistEntry> whiteList, @Nonnull Set<String> auditTablesInDatabase)
	{
		this.whiteList = whiteList;
		this.auditTablesInDatabase = auditTablesInDatabase;
	}

	@Parameterized(name = "auditTablesInDatabase", uniqueIdentifier = "auditTablesInDatabase")
	public static List<Object[]> generateData(@Nonnull @ConnectionProvider ConnectionProviderInstance connectionProvider, @Nonnull @WhiteList Map<String, WhitelistEntry> whiteList) throws SQLException, DataSetException
	{
		return Collections.singletonList(new Object[] { whiteList, connectionProvider.getQueries().getTablesByNameEndingWith(connectionProvider.getQueries().getAuditTablePostFix()) });
	}

	/**
	 * Validates that all audit tables present in the database are specified in the whitelist.
	 */
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
}