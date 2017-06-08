package org.tak.zeger.enversvalidationplugin.validate;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.dbunit.dataset.DataSetException;
import org.tak.zeger.enversvalidationplugin.annotation.ConnectionProvider;
import org.tak.zeger.enversvalidationplugin.annotation.TargetPhase;
import org.tak.zeger.enversvalidationplugin.annotation.Validate;
import org.tak.zeger.enversvalidationplugin.annotation.ValidationType;
import org.tak.zeger.enversvalidationplugin.annotation.WhiteList;
import org.tak.zeger.enversvalidationplugin.connection.ConnectionProviderInstance;
import org.tak.zeger.enversvalidationplugin.exceptions.ValidationException;

@ValidationType(TargetPhase.CONSTRAINTS)
public class ForeignKeyConstraintValidator
{
	@WhiteList
	private Map<String, String> whiteList;

	@ConnectionProvider
	private ConnectionProviderInstance connectionProvider;

	@Validate
	public void testNoForeignKeysExistsForNonWhiteListedTables() throws SQLException, DataSetException
	{
		final Set<String> auditTablesInDatabase = connectionProvider.getQueries().getListOfTablesWithForeignKeysToRevisionTable();
		final Set<String> tablesWithForeignKeyButNotWhiteListed = new HashSet<>(auditTablesInDatabase.size());
		for (String tableName : auditTablesInDatabase)
		{
			if (!whiteList.keySet().contains(tableName))
			{
				tablesWithForeignKeyButNotWhiteListed.add(tableName);
			}
		}

		if (!tablesWithForeignKeyButNotWhiteListed.isEmpty())
		{
			throw new ValidationException("Tables found with a reference to the revision table, which are not on the white list:" + tablesWithForeignKeyButNotWhiteListed);
		}
	}

	@Validate
	public void testAllAuditTablesHaveAForeignKeyToRevisionTable() throws SQLException, DataSetException
	{
		final Set<String> auditTablesWithoutForeignKeyToRevisionTable = new HashSet<>(whiteList.size());
		for (String auditTable : whiteList.keySet())
		{
			if (!connectionProvider.getQueries().getListOfTablesWithForeignKeysToRevisionTable().contains(auditTable))
			{
				auditTablesWithoutForeignKeyToRevisionTable.add(auditTable);
			}
		}

		if (!auditTablesWithoutForeignKeyToRevisionTable.isEmpty())
		{
			throw new ValidationException("Whitelisted audit tables found without a foreign key to the revision table" + auditTablesWithoutForeignKeyToRevisionTable);
		}
	}
}