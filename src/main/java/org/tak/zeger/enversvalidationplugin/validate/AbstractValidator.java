package org.tak.zeger.enversvalidationplugin.validate;

import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import org.tak.zeger.enversvalidationplugin.annotation.ConnectionProvider;
import org.tak.zeger.enversvalidationplugin.annotation.ListOfAuditTablesInDatabase;
import org.tak.zeger.enversvalidationplugin.annotation.WhiteList;
import org.tak.zeger.enversvalidationplugin.connection.ConnectionProviderInstance;

public abstract class AbstractValidator
{
	@ConnectionProvider
	private ConnectionProviderInstance connectionProvider;

	@WhiteList
	private Map<String, String> whiteList;

	@ListOfAuditTablesInDatabase
	private Set<String> auditTablesInDatabase;

	@Nonnull
	protected ConnectionProviderInstance getConnectionProvider()
	{
		return connectionProvider;
	}

	@Nonnull
	protected Map<String, String> getWhiteList()
	{
		return whiteList;
	}

	@Nonnull
	protected Set<String> getAuditTablesInDatabase()
	{
		return auditTablesInDatabase;
	}
}
