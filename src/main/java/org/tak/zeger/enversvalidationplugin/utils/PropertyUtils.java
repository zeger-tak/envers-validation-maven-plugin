package org.tak.zeger.enversvalidationplugin.utils;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.apache.commons.lang3.StringUtils;
import org.apache.maven.plugin.MojoFailureException;
import org.tak.zeger.enversvalidationplugin.connection.ConnectionProviderInstance;
import org.tak.zeger.enversvalidationplugin.entities.WhitelistEntry;
import org.tak.zeger.enversvalidationplugin.whitelist.ObjectFactory;
import org.tak.zeger.enversvalidationplugin.whitelist.WhiteListEntryFile;
import org.tak.zeger.enversvalidationplugin.whitelist.WhitelistEntryType;

public final class PropertyUtils
{
	private PropertyUtils()
	{
	}

	@Nonnull
	public static ConnectionProviderInstance getConnectionProperties(@Nonnull File file) throws MojoFailureException
	{
		final Properties connectionPropertiesInFile = getPropertiesFromFile(file);
		return createConnectionProvider(connectionPropertiesInFile);
	}

	@Nonnull
	private static ConnectionProviderInstance createConnectionProvider(@Nonnull Properties connectionPropertiesInFile) throws MojoFailureException
	{
		final String usernamePropertyKey = "username";
		final String passwordPropertyKey = "password";
		final String driverClassPropertyKey = "driver";
		final String connectionUrlPropertyKey = "url";
		final String whiteListPropertyFilePropertyKey = "whiteListPropertyFile";

		final String username = connectionPropertiesInFile.getProperty(usernamePropertyKey);
		final String password = connectionPropertiesInFile.getProperty(passwordPropertyKey);
		final String driverClass = connectionPropertiesInFile.getProperty(driverClassPropertyKey);
		final String connectionUrl = connectionPropertiesInFile.getProperty(connectionUrlPropertyKey);
		final String whiteListPropertyFile = connectionPropertiesInFile.getProperty(whiteListPropertyFilePropertyKey);

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
		if (StringUtils.isBlank(whiteListPropertyFile))
		{
			propertyKeysMissing.add(whiteListPropertyFilePropertyKey);
		}
		if (!propertyKeysMissing.isEmpty())
		{
			throw new MojoFailureException("The following required connection are missing from the connection property file: " + propertyKeysMissing);
		}

		return new ConnectionProviderInstance(connectionUrl, driverClass, username, password, whiteListPropertyFile);
	}

	@Nonnull
	public static Map<String, WhitelistEntry> getWhiteList(@Nonnull String fileName, @Nonnull String auditTablePostFix) throws MojoFailureException
	{
		final File file = new File(fileName);
		try
		{
			final JAXBContext jaxbContext = JAXBContext.newInstance(ObjectFactory.class);
			final Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
			final WhiteListEntryFile whiteListEntryFile = (WhiteListEntryFile) unmarshaller.unmarshal(file);
			return createWhitelist(whiteListEntryFile, auditTablePostFix);
		}
		catch (JAXBException | RuntimeException e)
		{
			throw new MojoFailureException("Unable to retrieve whitelist, errormessage: " + e.getMessage(), e);
		}
	}

	@Nonnull
	private static Map<String, WhitelistEntry> createWhitelist(WhiteListEntryFile whiteListEntryFile, @Nonnull String auditTablePostFix) throws MojoFailureException
	{
		final Map<String, WhitelistEntryType> whitelistTypes = convertToMap(whiteListEntryFile.getWhitelistEntry());
		final Map<String, WhitelistEntry> whiteList = new HashMap<>();

		for (WhitelistEntryType whitelistEntryType : whitelistTypes.values())
		{
			final String auditTableName = whitelistEntryType.getAuditTableName();
			final String contentTableName = parseContentTableName(whitelistEntryType, auditTablePostFix);
			whiteList.putIfAbsent(auditTableName, new WhitelistEntry(auditTableName, contentTableName));
			final WhitelistEntry whitelistEntry = whiteList.get(auditTableName);

			final String auditTableParentName = whitelistEntryType.getAuditTableParentName();
			if (StringUtils.isNotBlank(auditTableParentName))
			{
				final WhitelistEntryType parentWhitelistEntryType = whitelistTypes.get(auditTableParentName);
				if (parentWhitelistEntryType == null)
				{
					throw new MojoFailureException("Unable to construct the whitelist tree as " + whitelistEntryType + " has a parent audit table for which no " + WhitelistEntryType.class.getSimpleName() + " was configured.");
				}

				whiteList.putIfAbsent(parentWhitelistEntryType.getAuditTableName(), new WhitelistEntry(parentWhitelistEntryType.getAuditTableName(), parseContentTableName(parentWhitelistEntryType, auditTablePostFix)));
				final WhitelistEntry parentWhitelistEntry = whiteList.get(auditTableParentName);
				whitelistEntry.setAuditTableParent(parentWhitelistEntry);
			}
		}
		return whiteList;
	}

	@Nonnull
	private static String parseContentTableName(@Nonnull WhitelistEntryType whitelistEntryType, @Nonnull String auditTablePostFix)
	{
		return StringUtils.isBlank(whitelistEntryType.getContentTableName()) ? whitelistEntryType.getAuditTableName().replaceAll(auditTablePostFix, "") : whitelistEntryType.getContentTableName();
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

	@Nonnull
	private static Map<String, WhitelistEntryType> convertToMap(@Nonnull List<WhitelistEntryType> list)
	{
		return list.stream().collect(Collectors.toMap(WhitelistEntryType::getAuditTableName, Function.identity()));
	}
}
