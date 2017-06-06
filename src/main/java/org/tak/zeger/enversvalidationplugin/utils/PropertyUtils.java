package org.tak.zeger.enversvalidationplugin.utils;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.StringUtils;
import org.apache.maven.plugin.MojoFailureException;
import org.tak.zeger.enversvalidationplugin.connection.ConnectionProviderInstance;

public final class PropertyUtils
{
	private PropertyUtils()
	{
	}

	@Nonnull
	public static ConnectionProviderInstance getConnectionProperties(@Nonnull File file) throws MojoFailureException
	{
		final Properties connectionPropertiesInFile = getPropertiesFromFile(file);
		final ConnectionProviderInstance connectionProvider = createConnectionProvider(connectionPropertiesInFile);

		final String auditTablePostFixPropertyKey = "auditTablePostFix";
		final String auditTablePostFix = connectionPropertiesInFile.getProperty(auditTablePostFixPropertyKey);
		if (StringUtils.isNotBlank(auditTablePostFix))
		{
			connectionProvider.setAuditTablePostFix(auditTablePostFix);
		}
		return connectionProvider;
	}

	@Nonnull
	private static ConnectionProviderInstance createConnectionProvider(@Nonnull Properties connectionPropertiesInFile) throws MojoFailureException
	{
		final String usernamePropertyKey = "username";
		final String passwordPropertyKey = "password";
		final String driverClassPropertyKey = "driver";
		final String connectionUrlPropertyKey = "url";

		final String username = connectionPropertiesInFile.getProperty(usernamePropertyKey);
		final String password = connectionPropertiesInFile.getProperty(passwordPropertyKey);
		final String driverClass = connectionPropertiesInFile.getProperty(driverClassPropertyKey);
		final String connectionUrl = connectionPropertiesInFile.getProperty(connectionUrlPropertyKey);

		final List<String> propertyKeysMissing = new ArrayList<>(4);
		if (StringUtils.isBlank(username))
		{
			propertyKeysMissing.add(usernamePropertyKey);
		}
		if (StringUtils.isBlank(password))
		{
			propertyKeysMissing.add(passwordPropertyKey);
		}
		if (StringUtils.isBlank(driverClass))
		{
			propertyKeysMissing.add(driverClassPropertyKey);
		}
		if (StringUtils.isBlank(connectionUrl))
		{
			propertyKeysMissing.add(connectionUrlPropertyKey);
		}
		if (!propertyKeysMissing.isEmpty())
		{
			throw new MojoFailureException("The following required connection are missing from the connection property file: " + propertyKeysMissing);
		}

		return new ConnectionProviderInstance(connectionUrl, driverClass, username, password);
	}

	@Nonnull
	public static Map<String, String> getWhiteList(@Nonnull File file, @Nonnull String auditTablePostFix) throws MojoFailureException
	{
		Properties whiteList = getPropertiesFromFile(file);

		Map<String, String> map = new HashMap<>();
		for (final String name : whiteList.stringPropertyNames())
		{
			final String property = whiteList.getProperty(name);
			final String auditedTableName = StringUtils.isBlank(property) ? name.replaceAll(auditTablePostFix, "") : property;
			map.put(name, auditedTableName);
		}

		return map;
	}

	@Nonnull
	private static Properties getPropertiesFromFile(@Nonnull File file) throws MojoFailureException
	{
		Properties connectionPropertiesInFile = new Properties();

		try
		{
			connectionPropertiesInFile.load(new FileReader(file));
		}
		catch (IOException e)
		{
			throw new MojoFailureException(e.getMessage(), e);
		}
		return connectionPropertiesInFile;
	}
}
