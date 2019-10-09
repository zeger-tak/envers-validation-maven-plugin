package com.github.zeger_tak.enversvalidationplugin.validate;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.github.zeger_tak.enversvalidationplugin.annotation.AuditTableInformationMap;
import com.github.zeger_tak.enversvalidationplugin.annotation.ConnectionProvider;
import com.github.zeger_tak.enversvalidationplugin.annotation.Parameterized;
import com.github.zeger_tak.enversvalidationplugin.annotation.TargetPhase;
import com.github.zeger_tak.enversvalidationplugin.annotation.Validate;
import com.github.zeger_tak.enversvalidationplugin.annotation.ValidationType;
import com.github.zeger_tak.enversvalidationplugin.connection.ConnectionProviderInstance;
import com.github.zeger_tak.enversvalidationplugin.entities.AuditTableInformation;
import com.github.zeger_tak.enversvalidationplugin.exceptions.ValidationException;
import org.apache.commons.lang3.StringUtils;
import org.dbunit.dataset.DataSetException;

/**
 * The goal of this validator is described in its {@link Validate} methods:
 */
@ValidationType(TargetPhase.TABLE_STRUCTURE)
public class AuditTableInformationMapValidator
{
	private final Map<String, AuditTableInformation> auditTableInformationMap;
	private final Set<String> auditTablesInDatabase;

	@ConnectionProvider
	private ConnectionProviderInstance connectionProvider;

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

	@Validate
	public void validateAllContentTablesHaveAllColumnsInAuditTable() throws DataSetException, SQLException
	{
		final Set<String> missingAuditColumnNames = new HashSet<>();
		for (AuditTableInformation auditTableInformation : auditTableInformationMap.values())
		{
			final Set<String> contentTableColumns = connectionProvider.getQueries().getAllColumns(auditTableInformation.getContentTableName());
			final Set<String> auditTableColumns = connectionProvider.getQueries().getAllColumns(auditTableInformation.getAuditTableName());
			final Set<String> notAuditedColumns = auditTableInformation.getColumnNamesPresentInContentTableButNotInAuditTable();

			// @formatter:off
			contentTableColumns.stream()
				.filter(c -> !auditTableColumns.contains(c))
				.filter(c -> !notAuditedColumns.contains(c.toUpperCase()))
				.forEach(c -> missingAuditColumnNames.add(auditTableInformation.getAuditTableName() + "." + c));
			// @formatter:on
		}

		if (!missingAuditColumnNames.isEmpty())
		{
			throw new ValidationException("The following columns are missing: " + StringUtils.join(missingAuditColumnNames, ", "));
		}
	}
}