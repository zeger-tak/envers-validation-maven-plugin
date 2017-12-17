package org.tak.zeger.enversvalidationplugin.entities;

import java.util.Objects;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class WhitelistEntry
{
	private final String auditTableName;
	private final String auditTableParentName;
	private final String contentTableName;

	public WhitelistEntry(@Nonnull String auditTableName, @Nullable String auditTableParentName, @Nonnull String contentTableName)
	{
		this.auditTableName = auditTableName;
		this.auditTableParentName = auditTableParentName;
		this.contentTableName = contentTableName;
	}

	@Nonnull
	public String getAuditTableName()
	{
		return auditTableName;
	}

	@CheckForNull
	public String getAuditTableParentName()
	{
		return auditTableParentName;
	}

	@Nonnull
	public String getContentTableName()
	{
		return contentTableName;
	}

	@Override
	public boolean equals(Object o)
	{
		if (this == o)
		{
			return true;
		}
		if (o == null || getClass() != o.getClass())
		{
			return false;
		}
		WhitelistEntry that = (WhitelistEntry) o;
		return Objects.equals(auditTableName, that.auditTableName);
	}

	@Override
	public int hashCode()
	{
		return Objects.hash(auditTableName);
	}

	@Override
	public String toString()
	{
		return "WhitelistEntry[auditTableName='" + auditTableName + "', auditTableParentName='" + auditTableParentName + "', contentTableName='" + contentTableName + "']";
	}
}
