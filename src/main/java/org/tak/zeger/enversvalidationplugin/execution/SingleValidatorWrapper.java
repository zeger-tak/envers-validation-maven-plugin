package org.tak.zeger.enversvalidationplugin.execution;

import java.lang.reflect.Method;
import java.util.List;

import javax.annotation.Nonnull;

public class SingleValidatorWrapper implements ValidatorWrapper
{
	private final Object validator;
	private final List<Method> validateMethods;

	public SingleValidatorWrapper(@Nonnull Object validator, @Nonnull List<Method> validateMethods)
	{
		this.validator = validator;
		this.validateMethods = validateMethods;
	}

	@Nonnull
	@Override
	public Object getValidator()
	{
		return validator;
	}

	@Nonnull
	@Override
	public List<Method> getValidateMethods()
	{
		return validateMethods;
	}

	@Nonnull
	@Override
	public String getValidationName(@Nonnull Method method)
	{
		return validator.getClass().getSimpleName() + "." + method.getName();
	}

	@Nonnull
	@Override
	public String getUniqueIdentifier(@Nonnull Method method)
	{
		return getValidationName(method);
	}
}