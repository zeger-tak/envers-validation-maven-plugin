package org.tak.zeger.enversvalidationplugin.validate;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.dbunit.dataset.DataSetException;
import org.tak.zeger.enversvalidationplugin.annotation.AuditTableInformationMap;
import org.tak.zeger.enversvalidationplugin.annotation.ConnectionProvider;
import org.tak.zeger.enversvalidationplugin.annotation.Parameterized;
import org.tak.zeger.enversvalidationplugin.annotation.TargetPhase;
import org.tak.zeger.enversvalidationplugin.annotation.Validate;
import org.tak.zeger.enversvalidationplugin.annotation.ValidationType;
import org.tak.zeger.enversvalidationplugin.connection.ConnectionProviderInstance;
import org.tak.zeger.enversvalidationplugin.entities.AuditTableInformation;
import org.tak.zeger.enversvalidationplugin.exceptions.ValidationException;

/**
 * The goal of this validator is described in its {@link Validate} methods:
 */
@ValidationType(TargetPhase.TABLE_STRUCTURE)
public class AuditTableInformationMapValidator
{
	private final Map<String, AuditTableInformation> auditTableInformationMap;
	private final Set<String> auditTablesInDatabase;

	public AuditTableInformationMapValidator(@Nonnull Map<String, AuditTableInformation> auditTableInformationMap, @Nonnull Set<String> auditTablesInDatabase)
	{
		this.auditTableInformationMap = auditTableInformationMap;
		this.auditTablesInDatabase = auditTablesInDatabase;
	}

	@Parameterized(name = "auditTablesInDatabase", uniqueIdentifier = "auditTablesInDatabase")
	public static List<Object[]> generateData(@Nonnull @ConnectionProvider ConnectionProviderInstance connectionProvider, @Nonnull @AuditTableInformationMap Map<String, AuditTableInformation> auditTableInformationMap) throws SQLException, DataSetException
	{
		return Collections.singletonList(new Object[] { auditTableInformationMap, connectionProvider.getQueries().getTablesByNameEndingWith(connectionProvider.getQueries().getAuditTablePostFix()) });
	}

	/**
	 * Validates that all audit tables present in the database are specified in the audit table information.
	 */
	@Validate
	public void validateAllExistingAuditTablesAreSpecified()
	{
		final Set<String> auditTableInformationSpecified = auditTableInformationMap.keySet();

		final Set<String> auditTablesNotSpecified = auditTablesInDatabase.stream().map(String::toUpperCase).collect(Collectors.toSet());
		auditTablesNotSpecified.removeAll(auditTableInformationSpecified);

		if (!auditTablesNotSpecified.isEmpty())
		{
			throw new ValidationException("The following audit tables are not configured in the audit table information map: " + auditTablesNotSpecified);
		}
	}
}