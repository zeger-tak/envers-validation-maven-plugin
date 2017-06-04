package org.tak.zeger.enversvalidationplugin;

import java.io.File;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.dbunit.database.CachedResultSetTable;
import org.tak.zeger.enversvalidationplugin.connection.ConnectionProviderInstance;
import org.tak.zeger.enversvalidationplugin.exceptions.DatabaseNotSupportedException;
import org.tak.zeger.enversvalidationplugin.execution.ValidationExecutor;
import org.tak.zeger.enversvalidationplugin.utils.PropertyUtils;

@Mojo(name = "validate")
public class EnversValidationMojo extends AbstractMojo
{
	@Parameter(property = "connectionPropertyFile", required = true, readonly = true)
	private File connectionPropertyFile;

	@Parameter(property = "whiteListPropertyFile", required = true, readonly = true)
	private File whiteListPropertyFile;

	@Parameter(property = "auditTablePostFix", readonly = true, defaultValue = "_AUD")
	private String auditTablePostFix;

	@Override
	public void execute() throws MojoExecutionException, MojoFailureException
	{
		final ConnectionProviderInstance connectionProvider = PropertyUtils.getConnectionProperties(connectionPropertyFile);
		final Map<String, String> whiteList = PropertyUtils.getWhiteList(whiteListPropertyFile, auditTablePostFix);

		final Set<String> listOfAuditTablesInDatabase = getListOfAuditTablesInDatabase(connectionProvider, auditTablePostFix);

		try
		{
			final List<String> packagesToScanForExecutors = Collections.singletonList("org.tak.zeger.enversvalidationplugin.validate");
			final ValidationExecutor validationExecutor = new ValidationExecutor(getLog(), packagesToScanForExecutors, connectionProvider);
			validationExecutor.executeValidations(whiteList, listOfAuditTablesInDatabase);
		}
		catch (Exception e)
		{
			throw new MojoFailureException(e.getMessage(), e);
		}
	}

	@Nonnull
	private Set<String> getListOfAuditTablesInDatabase(ConnectionProviderInstance connectionProvider, @Nonnull String auditTablePostFix)
	{
		final String query;
		if (connectionProvider.isOracle())
		{
			query = "select TABLE_NAME from USER_TABLES where TABLE_NAME like '%" + auditTablePostFix + "'";
		}
		else if (connectionProvider.isPostgreSQL())
		{
			query = "select upper(table_name) table_name from information_schema.tables where UPPER(TABLE_NAME) like '%" + auditTablePostFix + "'";
		}
		else
		{
			throw new DatabaseNotSupportedException("Unable to start tests due to unsupported database type.");
		}
		try
		{
			final CachedResultSetTable allKnownTablesEndingWithPostFix = (CachedResultSetTable) connectionProvider.getDatabaseConnection().createQueryTable("USER_TABLES", query);

			final Set<String> auditTablesInDatabase = new HashSet<>(allKnownTablesEndingWithPostFix.getRowCount());
			for (int i = 0; i < allKnownTablesEndingWithPostFix.getRowCount(); i++)
			{
				auditTablesInDatabase.add((String) allKnownTablesEndingWithPostFix.getValue(i, "TABLE_NAME"));
			}

			return auditTablesInDatabase;
		}
		catch (Exception e)
		{
			throw new RuntimeException(e.getMessage(), e);
		}
	}
}