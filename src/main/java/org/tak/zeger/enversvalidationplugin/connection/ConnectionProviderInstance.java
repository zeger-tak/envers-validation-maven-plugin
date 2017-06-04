package org.tak.zeger.enversvalidationplugin.connection;

import javax.annotation.Nonnull;

import org.dbunit.IDatabaseTester;
import org.dbunit.JdbcDatabaseTester;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.ext.oracle.OracleDataTypeFactory;
import org.dbunit.ext.postgresql.PostgresqlDataTypeFactory;
import org.tak.zeger.enversvalidationplugin.exceptions.DatabaseNotSupportedException;

public class ConnectionProviderInstance
{
	private final String driverClass;
	private final String connectionUrl;
	private final String username;
	private final String password;
	private final IDatabaseTester databaseTester;

	private IDatabaseConnection databaseConnection;

	public ConnectionProviderInstance(@Nonnull String connectionUrl, @Nonnull String driverClass, @Nonnull String username, @Nonnull String password)
	{
		this.driverClass = driverClass;
		this.connectionUrl = connectionUrl;
		this.password = password;
		this.username = username;
		databaseTester = newDatabaseTester();
	}

	@Nonnull
	private IDatabaseTester newDatabaseTester()
	{
		try
		{
			if (isOracle())
			{
				// For Oracle, provide the schema name. Otherwise, metadata for ALL objects is retrieved.
				final JdbcDatabaseTester jdbcDatabaseTester = new JdbcDatabaseTester(driverClass, connectionUrl, username, password, username);
				databaseConnection = jdbcDatabaseTester.getConnection();
				getDatabaseConnection().getConfig().setProperty(DatabaseConfig.PROPERTY_DATATYPE_FACTORY, new OracleDataTypeFactory());
				return jdbcDatabaseTester;
			}
			else if (isPostgreSQL())
			{
				final JdbcDatabaseTester jdbcDatabaseTester = new JdbcDatabaseTester(driverClass, connectionUrl, username, password)
				{
				};
				databaseConnection = jdbcDatabaseTester.getConnection();
				getDatabaseConnection().getConfig().setProperty(DatabaseConfig.PROPERTY_DATATYPE_FACTORY, new PostgresqlDataTypeFactory());
				return jdbcDatabaseTester;
			}
		}
		catch (Exception e)
		{
			throw new DatabaseNotSupportedException(e.getMessage(), e);
		}

		throw new DatabaseNotSupportedException("Unable to determine database type.");
	}

	@Nonnull
	public IDatabaseConnection getDatabaseConnection()
	{
		if (databaseConnection == null)
		{
			try
			{
				databaseConnection = databaseTester.getConnection();
			}
			catch (Exception e)
			{
				throw new DatabaseNotSupportedException("Database connection could not be established.", e);
			}
		}
		return databaseConnection;
	}

	public boolean isOracle()
	{
		return driverClass.equals("oracle.jdbc.OracleDriver");
	}

	public boolean isPostgreSQL()
	{
		return driverClass.equals("org.postgresql.Driver");
	}

	@Override
	public String toString()
	{
		return "ConnectionProperties{" + "driverClass='" + driverClass + '\'' + ", connectionUrl='" + connectionUrl + '\'' + ", username='" + username + '\'' + '}';
	}
}