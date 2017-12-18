package org.tak.zeger.enversvalidationplugin.execution;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.text.MessageFormat;
import java.util.List;

import javax.annotation.Nonnull;

import org.tak.zeger.enversvalidationplugin.annotation.Parameterized;

public class ParameterizedValidatorWrapper implements ValidatorWrapper
{
	private final Object validator;
	private final Object[] constructorArguments;
	private final List<Method> validateMethods;
	private final int index;

	public ParameterizedValidatorWrapper(@Nonnull Object validator, @Nonnull Object[] constructorArguments, @Nonnull List<Method> validateMethods, int index)
	{
		this.validator = validator;
		this.constructorArguments = constructorArguments;
		this.validateMethods = validateMethods;
		this.index = index;
	}

	@Nonnull
	@Override
	public Object getValidator()
	{
		return validator;
	}

	@Nonnull
	@Override
	public String getValidationName(@Nonnull Method method)
	{
		String methodBeingExecuted = determineMethodName(method);

		for (Method staticMethod : validator.getClass().getMethods())
		{
			final Parameterized parameterized = staticMethod.getAnnotation(Parameterized.class);
			if (parameterized != null && Modifier.isStatic(staticMethod.getModifiers()))
			{
				final String name = parameterized.name().replaceAll("\\{index\\}", Integer.toString(index));
				methodBeingExecuted = methodBeingExecuted + "." + name;
				methodBeingExecuted = MessageFormat.format(methodBeingExecuted, constructorArguments);
				break;
			}
		}

		return methodBeingExecuted;
	}

	@Nonnull
	@Override
	public String getUniqueIdentifier(@Nonnull Method method)
	{
		String methodBeingExecuted = determineMethodName(method);

		for (Method staticMethod : validator.getClass().getMethods())
		{
			final Parameterized parameterized = staticMethod.getAnnotation(Parameterized.class);
			if (parameterized != null && Modifier.isStatic(staticMethod.getModifiers()))
			{
				methodBeingExecuted = methodBeingExecuted + "." + parameterized.uniqueIdentifier();
				methodBeingExecuted = MessageFormat.format(methodBeingExecuted, constructorArguments);
				break;
			}
		}
		return methodBeingExecuted;
	}

	@Nonnull
	private String determineMethodName(@Nonnull Method method)
	{
		return validator.getClass().getSimpleName() + "." + method.getName();
	}

	@Nonnull
	@Override
	public List<Method> getValidateMethods()
	{
		return validateMethods;
	}
}