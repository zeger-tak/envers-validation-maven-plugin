package com.github.zeger_tak.enversvalidationplugin.exceptions;

import javax.annotation.Nonnull;

public class DatabaseNotSupportedException extends ValidationException
{
	public DatabaseNotSupportedException(@Nonnull String message)
	{
		super(message);
	}

	public DatabaseNotSupportedException(@Nonnull String message, @Nonnull Throwable cause)
	{
		super(message, cause);
	}
}