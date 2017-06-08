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
	private DatabaseQueries databaseQueries;

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
			if (driverClass.equals("oracle.jdbc.OracleDriver"))
			{
				// For Oracle, provide the schema name. Otherwise, metadata for ALL objects is retrieved.
				final JdbcDatabaseTester jdbcDatabaseTester = new JdbcDatabaseTester(driverClass, connectionUrl, username, password, username);
				databaseConnection = jdbcDatabaseTester.getConnection();
				getDatabaseConnection().getConfig().setProperty(DatabaseConfig.PROPERTY_DATATYPE_FACTORY, new OracleDataTypeFactory());

				databaseQueries = new OracleQueries(this);
				return jdbcDatabaseTester;
			}
			else if (driverClass.equals("org.postgresql.Driver"))
			{
				// For Postgresql
				final JdbcDatabaseTester jdbcDatabaseTester = new JdbcDatabaseTester(driverClass, connectionUrl, username, password);
				databaseConnection = jdbcDatabaseTester.getConnection();
				getDatabaseConnection().getConfig().setProperty(DatabaseConfig.PROPERTY_DATATYPE_FACTORY, new PostgresqlDataTypeFactory());

				databaseQueries = new PostgresQueries(this);
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

	@Nonnull
	public DatabaseQueries getQueries()
	{
		if (databaseQueries == null)
		{
			newDatabaseTester();
		}

		return databaseQueries;
	}

	@Override
	public String toString()
	{
		return "ConnectionProperties{" + "driverClass='" + driverClass + '\'' + ", connectionUrl='" + connectionUrl + '\'' + ", username='" + username + '\'' + '}';
	}
}