package org.tak.zeger.enversvalidationplugin.execution;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.maven.plugin.logging.Log;
import org.reflections.Reflections;
import org.reflections.scanners.FieldAnnotationsScanner;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ConfigurationBuilder;
import org.tak.zeger.enversvalidationplugin.annotation.ValidationType;
import org.tak.zeger.enversvalidationplugin.connection.ConnectionProviderInstance;
import org.tak.zeger.enversvalidationplugin.exceptions.ValidationException;
import org.tak.zeger.enversvalidationplugin.utils.ReflectionUtils;
import org.tak.zeger.enversvalidationplugin.utils.ValidationInvocator;

public class ValidationExecutor
{
	private final Log log;
	private final List<String> packagesToScanForValidators;
	private final ConnectionProviderInstance connectionProvider;

	public ValidationExecutor(@Nonnull Log log, @Nonnull List<String> packagesToScanForValidators, @Nonnull ConnectionProviderInstance connectionProvider)
	{
		this.log = log;
		this.packagesToScanForValidators = packagesToScanForValidators;
		this.connectionProvider = connectionProvider;
	}

	public void executeValidations(@Nonnull Map<String, String> whiteList, @Nonnull Set<String> auditTablesInDatabase)
	{
		Reflections reflections = new Reflections(new ConfigurationBuilder().setUrls(ReflectionUtils.getPackages(packagesToScanForValidators)).setScanners(new SubTypesScanner(), new FieldAnnotationsScanner(), new TypeAnnotationsScanner()));
		Set<Class<?>> allValidators = reflections.getTypesAnnotatedWith(ValidationType.class);

		boolean exceptionsEncountered = false;
		final List<String> validatorsNotExecuted = new ArrayList<>(allValidators.size());
		for (Class<?> validatorClass : allValidators)
		{
			try
			{
				final boolean result = ValidationInvocator.invokeValidationMethods(log, validatorClass, connectionProvider, whiteList, auditTablesInDatabase);
				if (result)
				{
					exceptionsEncountered = true;
				}
			}
			catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e)
			{
				validatorsNotExecuted.add(validatorClass.getCanonicalName());
			}
		}

		validateResult(exceptionsEncountered, validatorsNotExecuted);
	}

	private static void validateResult(boolean exceptionsEncountered, List<String> validatorsNotExecuted)
	{
		final StringBuilder exceptionMessage = new StringBuilder();
		if (exceptionsEncountered)
		{
			exceptionMessage.append("Some of the validations failed, see log above for details.");
		}
		if (!validatorsNotExecuted.isEmpty())
		{
			if (exceptionsEncountered)
			{
				exceptionMessage.append(" ");
			}
			exceptionMessage.append("The following validators were not executed: ");
			exceptionMessage.append(validatorsNotExecuted);
		}

		if (exceptionMessage.length() > 0)
		{
			throw new ValidationException(exceptionMessage.toString());
		}
	}
}