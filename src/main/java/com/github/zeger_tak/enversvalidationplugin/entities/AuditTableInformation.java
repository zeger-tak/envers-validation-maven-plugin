package com.github.zeger_tak.enversvalidationplugin.entities;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class AuditTableInformation
{
	private final String auditTableName;
	private final String contentTableName;
	private final Set<String> columnNamesPresentInContentTableButNotInAuditTable;

	private AuditTableInformation auditTableParent;

	public AuditTableInformation(@Nonnull String auditTableName, @Nonnull String contentTableName)
	{
		this(auditTableName, contentTableName, Collections.emptySet());
	}

	public AuditTableInformation(@Nonnull String auditTableName, @Nonnull String contentTableName, @Nonnull Set<String> columnNamesPresentInContentTableButNotInAuditTable)
	{
		this.auditTableName = auditTableName;
		this.contentTableName = contentTableName;
		this.columnNamesPresentInContentTableButNotInAuditTable = columnNamesPresentInContentTableButNotInAuditTable;
	}

	@Nonnull
	public String getAuditTableName()
	{
		return auditTableName;
	}

	@CheckForNull
	public AuditTableInformation getAuditTableParent()
	{
		return auditTableParent;
	}

	public void setAuditTableParent(@Nullable AuditTableInformation auditTableParent)
	{
		this.auditTableParent = auditTableParent;
	}

	@Nonnull
	public String getContentTableName()
	{
		return contentTableName;
	}

	@Nonnull
	public Set<String> getColumnNamesPresentInContentTableButNotInAuditTable()
	{
		final Set<String> allColumnNamesPresentInContentTableButNotInAuditTable = new HashSet<>(columnNamesPresentInContentTableButNotInAuditTable);
		if (auditTableParent != null)
		{
			allColumnNamesPresentInContentTableButNotInAuditTable.addAll(auditTableParent.getColumnNamesPresentInContentTableButNotInAuditTable());
		}
		return Collections.unmodifiableSet(allColumnNamesPresentInContentTableButNotInAuditTable);
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
		AuditTableInformation that = (AuditTableInformation) o;
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
		return "AuditTableInformation[auditTableName='" + auditTableName + (auditTableParent == null ? "" : "', auditTableParent='" + auditTableParent) + "', contentTableName='" + contentTableName + "']";
	}
}
