package org.tak.zeger.enversvalidationplugin.execution;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.text.MessageFormat;

import javax.annotation.Nonnull;

import org.tak.zeger.enversvalidationplugin.annotation.Parameterized;

public class ParameterizedValidatorWrapper implements ValidatorWrapper
{
	private final int index;
	private final Object validator;
	private final Object[] constructorArguments;

	public ParameterizedValidatorWrapper(@Nonnull Object validator, @Nonnull Object[] constructorArguments, int index)
	{
		this.validator = validator;
		this.constructorArguments = constructorArguments;
		this.index = index;
	}

	@Nonnull
	public Object getValidator()
	{
		return validator;
	}

	@Nonnull
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
}