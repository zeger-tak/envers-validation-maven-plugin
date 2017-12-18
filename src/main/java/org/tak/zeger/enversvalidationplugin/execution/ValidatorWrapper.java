package org.tak.zeger.enversvalidationplugin.execution;

import java.lang.reflect.Method;
import java.util.List;

import javax.annotation.Nonnull;

public interface ValidatorWrapper
{
	@Nonnull
	Object getValidator();

	@Nonnull
	String getValidationName(@Nonnull Method method);

	@Nonnull
	String getUniqueIdentifier(@Nonnull Method method);

	@Nonnull
	List<Method> getValidateMethods();
}