package org.tak.zeger.enversvalidationplugin.validate;

import java.sql.SQLException;
import java.util.ArrayList;
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
import org.tak.zeger.enversvalidationplugin.exceptions.ValidationException;

@ValidationType(TargetPhase.CONSTRAINTS)
public class NullableColumnsValidator
{
	private final String tableName;
	private final List<String> primaryIdentifierColumnNames;
	private final Set<String> nonNullColumns;

	public NullableColumnsValidator(@Nonnull String tableName, @Nonnull List<String> primaryIdentifierColumnNames, @Nonnull Set<String> nonNullColumns)
	{
		this.tableName = tableName;
		this.primaryIdentifierColumnNames = primaryIdentifierColumnNames;
		this.nonNullColumns = nonNullColumns;
	}

	@Parameterized(name = "{index}: auditTableName: {0}", uniqueIdentifier = "{0}")
	public static List<Object[]> generateTestData(@Nonnull @ConnectionProvider ConnectionProviderInstance connectionProvider, @Nonnull @WhiteList Map<String, String> whiteList) throws SQLException, DataSetException
	{
		final List<Object[]> testData = new ArrayList<>();
		for (Map.Entry<String, String> whiteListEntry : whiteList.entrySet())
		{
			final List<String> primaryIdentifierColumnNames = connectionProvider.getQueries().getPrimaryKeyColumnNames(whiteListEntry.getValue());
			final Set<String> nonNullColumns = connectionProvider.getQueries().getAllNonnullColumns(whiteListEntry.getKey());

			testData.add(new Object[] { whiteListEntry.getValue(), primaryIdentifierColumnNames, nonNullColumns });
		}

		return testData;
	}

	@Validate
	public void testAllColumnsExceptPrimaryKeyAreNullable()
	{
		final Set<String> invalidNonnullColumnNames = nonNullColumns.stream().filter(c -> !primaryIdentifierColumnNames.contains(c)).collect(Collectors.toSet());

		if (!invalidNonnullColumnNames.isEmpty())
		{
			throw new ValidationException("The following columns for table " + tableName + " have a not null constraint which prevents remove revisions: " + invalidNonnullColumnNames);
		}
	}
}