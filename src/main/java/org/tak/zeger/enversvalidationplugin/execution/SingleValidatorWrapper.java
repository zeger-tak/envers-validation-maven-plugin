package org.tak.zeger.enversvalidationplugin.execution;

import java.lang.reflect.Method;

import javax.annotation.Nonnull;

public class SingleValidatorWrapper implements ValidatorWrapper
{
	private final Object validator;

	public SingleValidatorWrapper(@Nonnull Object validator)
	{
		this.validator = validator;
	}

	@Nonnull
	public Object getValidator()
	{
		return validator;
	}

	@Nonnull
	public String getValidationName(@Nonnull Method method)
	{
		return validator.getClass().getCanonicalName() + "." + method.getName();
	}
}