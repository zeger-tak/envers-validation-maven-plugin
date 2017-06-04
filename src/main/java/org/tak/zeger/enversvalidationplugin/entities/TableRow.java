package org.tak.zeger.enversvalidationplugin.entities;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

public class TableRow
{
	private final Map<String, Object> columns = new HashMap<>();

	public void addColumn(@Nonnull String columnName, @Nonnull Object value)
	{
		columns.put(columnName, value);
	}

	@CheckForNull
	public Object getColumnValue(@Nonnull String columnName)
	{
		return columns.get(columnName);
	}

	@Nonnull
	public Set<String> getColumnNames()
	{
		return columns.keySet();
	}

	@Override
	public String toString()
	{
		return "TableRow[" + "columns=" + columns + ']';
	}
}