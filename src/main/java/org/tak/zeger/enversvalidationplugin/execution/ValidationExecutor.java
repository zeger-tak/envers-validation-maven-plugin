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
import org.tak.zeger.enversvalidationplugin.entities.Config;
import org.tak.zeger.enversvalidationplugin.exceptions.ValidationException;
import org.tak.zeger.enversvalidationplugin.utils.ReflectionUtils;

/**
 * This class is NOT thread-safe by design, so do not call 2 public methods from different contexts.
 */
public class ValidationExecutor
{
	private final Log log;
	private final ConnectionProviderInstance connectionProvider;
	private final Config config;

	// Results
	private final List<String> validatorsExecutionFailed = new ArrayList<>();
	private final List<Class> validatorClassesIgnored = new ArrayList<>();
	private final List<Method> validatorMethodsIgnored = new ArrayList<>();
	private int failedTests = 0;

	public ValidationExecutor(@Nonnull Log log, @Nonnull Config config, @Nonnull ConnectionProviderInstance connectionProvider)
	{
		this.log = log;
		this.config = config;
		this.connectionProvider = connectionProvider;
	}

	public void executeValidations(@Nonnull Log log, @Nonnull Set<String> auditTablesInDatabase)
	{
		clearResults();

		Reflections reflections = new Reflections(new ConfigurationBuilder().setUrls(ReflectionUtils.getPackages(config.getPackagesToScanForValidators())).setScanners(new SubTypesScanner(), new FieldAnnotationsScanner(), new TypeAnnotationsScanner()));
		Set<Class<?>> allValidators = reflections.getTypesAnnotatedWith(ValidationType.class);

		for (Class<?> validatorClass : allValidators)
		{
			if (config.validationShouldBeIgnored(validatorClass))
			{
				validatorClassesIgnored.add(validatorClass);
				continue;
			}

			try
			{
				invokeValidationValidators(log, validatorClass, connectionProvider, config.getWhiteList(), auditTablesInDatabase);
			}
			catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e)
			{
				validatorsExecutionFailed.add(validatorClass.getCanonicalName());
			}
		}
		validateResult(log);
	}

	private void clearResults()
	{
		failedTests = 0;
		validatorClassesIgnored.clear();
		validatorsExecutionFailed.clear();
		validatorMethodsIgnored.clear();
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
				if (method.isAnnotationPresent(Validate.class))
				{
					if (config.validationShouldBeIgnored(wrapper, method))
					{
						log.info("Ignored validation method " + wrapper.getValidationName(method));
						validatorMethodsIgnored.add(method);
						continue;
					}

					log.debug("Started with " + wrapper.getValidationName(method));
					final String validationName = wrapper.getValidationName(method);
					try
					{
						method.invoke(validatorInstance);
						log.debug(validationName + " executed sucessfully.");
					}
					catch (IllegalAccessException | InvocationTargetException | ValidationException e)
					{
						if (e.getCause() instanceof ValidationException)
						{
							log.error(validationName + " failed, with the following message: " + e.getCause().getMessage());
						}
						else
						{
							log.error(validationName + " failed, with the following message: " + e.getMessage());
						}
						failedTests++;
					}
					log.debug("Finished with " + wrapper.getValidationName(method));
				}
			}
		}
	}

	private void validateResult(@Nonnull Log log)
	{
		if (!validatorClassesIgnored.isEmpty())
		{
			log.info("The following validators were ignored: " + validatorClassesIgnored);
		}

		final StringBuilder exceptionMessage = new StringBuilder();
		if (failedTests > 0)
		{
			exceptionMessage.append(failedTests);
			exceptionMessage.append(" validations failed, see log above for details.");
		}
		if (!validatorsExecutionFailed.isEmpty())
		{
			if (exceptionMessage.length() > 0)
			{
				exceptionMessage.append(" ");
			}
			exceptionMessage.append("The following validators were not succesfully executed: ");
			exceptionMessage.append(validatorsExecutionFailed);
		}

		if (exceptionMessage.length() > 0)
		{
			throw new ValidationException(exceptionMessage.toString());
		}
	}
}