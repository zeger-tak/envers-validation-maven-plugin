package com.github.zeger_tak.enversvalidationplugin.exceptions;

import javax.annotation.Nonnull;

public class ValidationException extends RuntimeException
{
	public ValidationException(@Nonnull String message)
	{
		super(message);
	}

	public ValidationException(@Nonnull String message, @Nonnull Throwable cause)
	{
		super(message, cause);
	}
}