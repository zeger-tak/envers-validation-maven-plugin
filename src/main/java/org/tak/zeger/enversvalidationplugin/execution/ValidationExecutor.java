package org.tak.zeger.enversvalidationplugin.execution;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
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
import org.tak.zeger.enversvalidationplugin.annotation.Validate;
import org.tak.zeger.enversvalidationplugin.annotation.ValidationType;
import org.tak.zeger.enversvalidationplugin.connection.ConnectionProviderInstance;
import org.tak.zeger.enversvalidationplugin.exceptions.ValidationException;
import org.tak.zeger.enversvalidationplugin.utils.ReflectionUtils;

/**
 * This class is NOT thread-safe by design, so do not call 2 public methods from different contexts.
 */
public class ValidationExecutor
{
	private final Log log;
	private final List<String> packagesToScanForValidators;
	private final ConnectionProviderInstance connectionProvider;

	// Results
	private final List<String> validatorsExecutionFailed = new ArrayList<>();
	private int failedTests = 0;

	public ValidationExecutor(@Nonnull Log log, @Nonnull List<String> packagesToScanForValidators, @Nonnull ConnectionProviderInstance connectionProvider)
	{
		this.log = log;
		this.packagesToScanForValidators = packagesToScanForValidators;
		this.connectionProvider = connectionProvider;
	}

	public void executeValidations(@Nonnull Map<String, String> whiteList, @Nonnull Set<String> auditTablesInDatabase)
	{
		clearResults();

		Reflections reflections = new Reflections(new ConfigurationBuilder().setUrls(ReflectionUtils.getPackages(packagesToScanForValidators)).setScanners(new SubTypesScanner(), new FieldAnnotationsScanner(), new TypeAnnotationsScanner()));
		Set<Class<?>> allValidators = reflections.getTypesAnnotatedWith(ValidationType.class);

		for (Class<?> validatorClass : allValidators)
		{
			try
			{
				invokeValidationValidators(log, validatorClass, connectionProvider, whiteList, auditTablesInDatabase);
			}
			catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e)
			{
				validatorsExecutionFailed.add(validatorClass.getCanonicalName());
			}
		}
		validateResult();
	}

	private void clearResults()
	{
		failedTests = 0;
		validatorsExecutionFailed.clear();
	}

	private void invokeValidationValidators(@Nonnull Log log, @Nonnull Class<?> validatorClass, @Nonnull ConnectionProviderInstance connectionProvider, @Nonnull Map<String, String> whiteList, @Nonnull Set<String> auditTablesInDatabase) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException
	{
		final ValidatorInstanceCreator validatorInstanceCreator = new ValidatorInstanceCreator(connectionProvider, whiteList, auditTablesInDatabase, validatorClass);
		final List<ValidatorWrapper> validatorInstances = validatorInstanceCreator.getValidators();

		for (ValidatorWrapper wrapper : validatorInstances)
		{
			Object validatorInstance = wrapper.getValidator();
			Method[] methods = validatorClass.getMethods();
			for (Method method : methods)
			{
				log.debug("Started with " + wrapper.getValidationName(method));
				if (method.isAnnotationPresent(Validate.class))
				{
					final String validationName = wrapper.getValidationName(method);
					try
					{
						method.invoke(validatorInstance);
						log.info(validationName + " executed sucessfully.");
					}
					catch (IllegalAccessException | InvocationTargetException | ValidationException e)
					{
						if (e.getCause() instanceof ValidationException)
						{
							log.error(validationName + " failed, due to: " + e.getCause().getMessage(), e.getCause());
						}
						else
						{
							log.error(validationName + " failed, due to: " + e.getMessage(), e);
						}
						failedTests++;
					}
				}
				log.debug("Finished with " + wrapper.getValidationName(method));
			}
		}
	}

	private void validateResult()
	{
		final StringBuilder exceptionMessage = new StringBuilder();
		if (failedTests > 0)
		{
			exceptionMessage.append(failedTests);
			exceptionMessage.append(" validations failed, see log above for details.");
		}
		if (!validatorsExecutionFailed.isEmpty())
		{
			if (failedTests > 0)
			{
				exceptionMessage.append(" ");
			}
			exceptionMessage.append("The following validators were not executed: ");
			exceptionMessage.append(validatorsExecutionFailed);
		}

		if (exceptionMessage.length() > 0)
		{
			throw new ValidationException(exceptionMessage.toString());
		}
	}
}