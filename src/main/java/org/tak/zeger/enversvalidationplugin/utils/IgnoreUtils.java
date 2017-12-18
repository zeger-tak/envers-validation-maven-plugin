package org.tak.zeger.enversvalidationplugin.utils;

import java.lang.reflect.Method;
import java.util.List;

import javax.annotation.Nonnull;

import org.tak.zeger.enversvalidationplugin.execution.ValidatorWrapper;

public final class IgnoreUtils
{
	private IgnoreUtils()
	{
	}

	public static boolean validationShouldBeIgnored(@Nonnull List<String> ignorables, @Nonnull Class<?> validatorClass)
	{
		return ignorables.contains(validatorClass.getSimpleName());
	}

	public static boolean validationShouldBeIgnored(@Nonnull List<String> ignorables, @Nonnull ValidatorWrapper validatorWrapper, @Nonnull Method validatorMethod)
	{
		return ignorables.contains(validatorWrapper.getUniqueIdentifier(validatorMethod));
	}
}