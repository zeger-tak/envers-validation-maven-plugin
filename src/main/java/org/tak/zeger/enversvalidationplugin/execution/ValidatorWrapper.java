package org.tak.zeger.enversvalidationplugin.execution;

import java.lang.reflect.Method;

import javax.annotation.Nonnull;

public interface ValidatorWrapper
{
	@Nonnull
	Object getValidator();

	@Nonnull
	String getValidationName(@Nonnull Method method);
}