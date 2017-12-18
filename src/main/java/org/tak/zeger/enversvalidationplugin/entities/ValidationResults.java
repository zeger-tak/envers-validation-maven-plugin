package org.tak.zeger.enversvalidationplugin.entities;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;

public class ValidationResults
{
	private final List<Class> validatorClassesIgnored = new ArrayList<>();
	private final List<Method> validatorMethodsIgnored = new ArrayList<>();
	private int executionsFailed;

	public void addClassToIgnored(@Nonnull Class<?> validatorClass)
	{
		validatorClassesIgnored.add(validatorClass);
	}

	public void addMethodToIgnored(@Nonnull Method validatorMethod)
	{
		validatorMethodsIgnored.add(validatorMethod);
	}

	public void addFailedExecution()
	{
		executionsFailed++;
	}

	@Nonnull
	public List<Class> getValidatorClassesIgnored()
	{
		return Collections.unmodifiableList(validatorClassesIgnored);
	}

	@Nonnull
	public List<Method> getValidatorMethodsIgnored()
	{
		return Collections.unmodifiableList(validatorMethodsIgnored);
	}

	public int getExecutionsFailed()
	{
		return executionsFailed;
	}
}