package org.tak.zeger.enversvalidationplugin.utils;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.maven.plugin.logging.Log;
import org.tak.zeger.enversvalidationplugin.annotation.ListOfAuditTablesInDatabase;
import org.tak.zeger.enversvalidationplugin.annotation.Parameterized;
import org.tak.zeger.enversvalidationplugin.annotation.Validate;
import org.tak.zeger.enversvalidationplugin.annotation.WhiteList;
import org.tak.zeger.enversvalidationplugin.connection.ConnectionProviderInstance;
import org.tak.zeger.enversvalidationplugin.exceptions.ValidationException;

public final class ValidationInvocator
{
	private ValidationInvocator()
	{
	}

	public static boolean invokeValidationMethods(@Nonnull Log log, @Nonnull Class<?> validatorClass, @Nonnull ConnectionProviderInstance connectionProvider, @Nonnull Map<String, String> whiteList, @Nonnull Set<String> auditTablesInDatabase) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException
	{
		final List<Object[]> parameterizedResult = invokeParameterizedMethod(validatorClass, connectionProvider, whiteList, auditTablesInDatabase);
		final List<Object> validatorInstances = createNewInstance(validatorClass, parameterizedResult, connectionProvider, whiteList, auditTablesInDatabase);

		boolean exceptionsEncountered = false;
		for (int index = 0; index < validatorInstances.size(); index++)
		{
			Object validatorInstance = validatorInstances.get(index);
			Method[] methods = validatorClass.getMethods();
			for (Method method : methods)
			{
				if (method.isAnnotationPresent(Validate.class))
				{
					final String validationName = createValidationName(validatorClass, method, index, parameterizedResult.isEmpty() ? new Object[] {} : parameterizedResult.get(index));
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
						exceptionsEncountered = true;
					}
				}
			}
		}

		return exceptionsEncountered;
	}

	@Nonnull
	private static String createValidationName(@Nonnull Class<?> validatorClass, @Nonnull Method method, int index, Object[] parameters)
	{
		String methodBeingExecuted = validatorClass.getCanonicalName() + "." + method.getName();

		for (Method staticMethod : validatorClass.getMethods())
		{
			final Parameterized parameterized = staticMethod.getAnnotation(Parameterized.class);
			if (parameterized != null && Modifier.isStatic(staticMethod.getModifiers()))
			{
				final String name = parameterized.name().replaceAll("\\{index\\}", Integer.toString(index));
				methodBeingExecuted = methodBeingExecuted + "." + name;
				methodBeingExecuted = MessageFormat.format(methodBeingExecuted, parameters);
				break;
			}
		}

		return methodBeingExecuted;
	}

	@Nonnull
	private static List<Object> createNewInstance(@Nonnull Class<?> validatorClass, List<Object[]> parameterizedResult, @Nonnull ConnectionProviderInstance connectionProvider, @Nonnull Map<String, String> whiteList, @Nonnull Set<String> auditTablesInDatabase) throws NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException
	{
		if (parameterizedResult.isEmpty())
		{
			return Collections.singletonList(createValidatorInstance(validatorClass, connectionProvider, whiteList, auditTablesInDatabase, new Object[] {}));
		}
		else
		{
			final List<Object> validatorInstances = new ArrayList<>(parameterizedResult.size());
			for (Object[] constructorParameters : parameterizedResult)
			{
				validatorInstances.add(createValidatorInstance(validatorClass, connectionProvider, whiteList, auditTablesInDatabase, constructorParameters));
			}

			return validatorInstances;
		}
	}

	@Nonnull
	private static Object createValidatorInstance(@Nonnull Class<?> validatorClass, @Nonnull ConnectionProviderInstance connectionProvider, @Nonnull Map<String, String> whiteList, @Nonnull Set<String> auditTablesInDatabase, @Nonnull Object[] constructorArguments) throws InstantiationException, IllegalAccessException, InvocationTargetException
	{
		final Object newInstance = validatorClass.getConstructors()[0].newInstance(constructorArguments);

		final List<Field> declaredFields = ReflectionUtils.getAllDeclaredFields(validatorClass);
		for (Field declaredField : declaredFields)
		{
			declaredField.setAccessible(true);

			final org.tak.zeger.enversvalidationplugin.annotation.ConnectionProvider connectionProviderAnnotation = declaredField.getAnnotation(org.tak.zeger.enversvalidationplugin.annotation.ConnectionProvider.class);
			final WhiteList whiteListAnnotation = declaredField.getAnnotation(WhiteList.class);
			final ListOfAuditTablesInDatabase listOfAuditTablesInDatabaseAnnotation = declaredField.getAnnotation(ListOfAuditTablesInDatabase.class);

			if (connectionProviderAnnotation != null)
			{
				declaredField.set(newInstance, connectionProvider);
			}
			else if (whiteListAnnotation != null)
			{
				declaredField.set(newInstance, whiteList);
			}
			else if (listOfAuditTablesInDatabaseAnnotation != null)
			{
				declaredField.set(newInstance, auditTablesInDatabase);
			}
		}

		return newInstance;
	}

	@Nonnull
	private static List<Object[]> invokeParameterizedMethod(@Nonnull Class<?> validatorClass, @Nonnull ConnectionProviderInstance connectionProvider, @Nonnull Map<String, String> whiteList, @Nonnull Set<String> auditTablesInDatabase)
	{
		for (Method method : validatorClass.getMethods())
		{
			final Parameterized parameterized = method.getAnnotation(Parameterized.class);
			if (parameterized != null && Modifier.isStatic(method.getModifiers()))
			{
				final List<Object> methodParameters = new ArrayList<>(method.getParameterCount());
				for (Parameter parameter : method.getParameters())
				{
					final org.tak.zeger.enversvalidationplugin.annotation.ConnectionProvider connectionProviderAnnotation = parameter.getAnnotation(org.tak.zeger.enversvalidationplugin.annotation.ConnectionProvider.class);
					final WhiteList whiteListAnnotation = parameter.getAnnotation(WhiteList.class);
					final ListOfAuditTablesInDatabase listOfAuditTablesInDatabaseAnnotation = parameter.getAnnotation(ListOfAuditTablesInDatabase.class);

					if (connectionProviderAnnotation != null)
					{
						methodParameters.add(connectionProvider);
					}
					else if (whiteListAnnotation != null)
					{
						methodParameters.add(whiteList);
					}
					else if (listOfAuditTablesInDatabaseAnnotation != null)
					{
						methodParameters.add(auditTablesInDatabase);
					}
				}

				try
				{
					final Object resultObject = method.invoke(null, methodParameters.toArray());
					List<Object[]> result = (List<Object[]>) resultObject;
					return result;
				}
				catch (ClassCastException e)
				{
					throw new ValidationException("Unable to create " + validatorClass.getCanonicalName() + " test, because parameterized method " + method.getName() + " has an invalid return type");
				}
				catch (IllegalAccessException | InvocationTargetException e)
				{
					throw new ValidationException("Unable to create " + validatorClass.getCanonicalName() + " test, because parameterized method " + method.getName() + " could not be invoked ", e);
				}
			}
		}
		return Collections.emptyList();
	}
}