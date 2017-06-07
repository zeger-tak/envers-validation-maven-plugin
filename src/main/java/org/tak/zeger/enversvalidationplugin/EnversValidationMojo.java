package org.tak.zeger.enversvalidationplugin;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.tak.zeger.enversvalidationplugin.connection.ConnectionProviderInstance;
import org.tak.zeger.enversvalidationplugin.entities.Config;
import org.tak.zeger.enversvalidationplugin.execution.ValidationExecutor;
import org.tak.zeger.enversvalidationplugin.utils.PropertyUtils;

@Mojo(name = "validate")
public class EnversValidationMojo extends AbstractMojo
{
	private static final String PACKAGE_TO_ALWAYS_SCAN_FOR_EXECUTORS = "org.tak.zeger.enversvalidationplugin.validate";

	@Parameter(property = "connectionPropertyFile", required = true, readonly = true)
	private File connectionPropertyFile;

	@Parameter(property = "whiteListPropertyFile", required = true, readonly = true)
	private File whiteListPropertyFile;

	@Parameter(property = "packageToScanForValidators", readonly = true)
	private List<String> packageToScanForValidators;

	@Parameter(property = "ignorable", readonly = true)
	private List<String> ignorable;

	@Override
	public void execute() throws MojoExecutionException, MojoFailureException
	{
		final ConnectionProviderInstance connectionProvider = PropertyUtils.getConnectionProperties(connectionPropertyFile);
		final Map<String, String> whiteList = PropertyUtils.getWhiteList(whiteListPropertyFile, connectionProvider.getAuditTablePostFix());

		final Set<String> listOfAuditTablesInDatabase = getListOfAuditTablesInDatabase(connectionProvider);
		final Config config = new Config(packageToScanForValidators, whiteList, ignorable);
		try
		{
			packageToScanForValidators.add(PACKAGE_TO_ALWAYS_SCAN_FOR_EXECUTORS);
			final ValidationExecutor validationExecutor = new ValidationExecutor(getLog(), config, connectionProvider);
			validationExecutor.executeValidations(listOfAuditTablesInDatabase);
		}
		catch (Exception e)
		{
			throw new MojoFailureException(e.getMessage(), e);
		}
	}

	@Nonnull
	private Set<String> getListOfAuditTablesInDatabase(@Nonnull ConnectionProviderInstance connectionProvider)
	{
		try
		{
			return connectionProvider.getQueries().getTablesByNameEndingWith(connectionProvider.getAuditTablePostFix());
		}
		catch (Exception e)
		{
			throw new RuntimeException(e.getMessage(), e);
		}
	}
}