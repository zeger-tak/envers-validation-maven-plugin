package com.github.zeger_tak.enversvalidationplugin.validate;

import java.sql.SQLException;
import java.util.ArrayList;
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
import org.dbunit.dataset.DataSetException;

/**
 * The goal of this validator is described in its {@link Validate} methods:
 */
@ValidationType(TargetPhase.CONSTRAINTS)
public class NullableColumnsValidator
{
	private final ConnectionProviderInstance connectionProvider;
	private final AuditTableInformation auditTableInformation;
	private final List<String> primaryIdentifierColumnNames;
	private final Set<String> nonNullColumns;

	public NullableColumnsValidator(@Nonnull ConnectionProviderInstance connectionProvider, @Nonnull AuditTableInformation auditTableInformation, @Nonnull List<String> primaryIdentifierColumnNames, @Nonnull Set<String> nonNullColumns)
	{
		this.connectionProvider = connectionProvider;
		this.auditTableInformation = auditTableInformation;
		this.primaryIdentifierColumnNames = primaryIdentifierColumnNames;
		this.nonNullColumns = nonNullColumns;
	}

	@Parameterized(name = "{index}: auditTableName: {1}", uniqueIdentifier = "{1}")
	public static List<Object[]> generateTestData(@Nonnull @ConnectionProvider ConnectionProviderInstance connectionProvider, @Nonnull @AuditTableInformationMap Map<String, AuditTableInformation> auditTableInformationMap) throws SQLException, DataSetException
	{
		final List<Object[]> testData = new ArrayList<>();
		for (Map.Entry<String, AuditTableInformation> auditTableInformation : auditTableInformationMap.entrySet())
		{
			final List<String> primaryIdentifierColumnNames = connectionProvider.getQueries().getPrimaryKeyColumnNames(auditTableInformation.getKey());
			final Set<String> nonNullColumns = connectionProvider.getQueries().getAllNonnullColumns(auditTableInformation.getKey());

			testData.add(new Object[] { connectionProvider, auditTableInformation.getValue(), primaryIdentifierColumnNames, nonNullColumns });
		}

		return testData;
	}

	/**
	 * Validates that all columns in the audit table are nullable except for the columns that make up the primary key.
	 */
	@Validate
	public void validateAllColumnsExceptPrimaryKeyAreNullable()
	{
		final String revisionTypeColumnName = connectionProvider.getQueries().getRevTypeColumnName();
		final Set<String> invalidNonnullColumnNames = nonNullColumns.stream().filter(c -> !primaryIdentifierColumnNames.contains(c) && !revisionTypeColumnName.equals(c)).collect(Collectors.toSet());

		if (!invalidNonnullColumnNames.isEmpty())
		{
			throw new ValidationException("The following columns for table " + auditTableInformation.getAuditTableName() + " have a not null constraint which prevents remove revisions: " + invalidNonnullColumnNames);
		}
	}
}