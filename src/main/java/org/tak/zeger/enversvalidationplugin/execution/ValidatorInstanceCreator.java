package org.tak.zeger.enversvalidationplugin.execution;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import org.tak.zeger.enversvalidationplugin.annotation.ConnectionProvider;
import org.tak.zeger.enversvalidationplugin.annotation.ListOfAuditTablesInDatabase;
import org.tak.zeger.enversvalidationplugin.annotation.Parameterized;
import org.tak.zeger.enversvalidationplugin.annotation.WhiteList;
import org.tak.zeger.enversvalidationplugin.connection.ConnectionProviderInstance;
import org.tak.zeger.enversvalidationplugin.entities.WhitelistEntry;
import org.tak.zeger.enversvalidationplugin.exceptions.ValidationException;
import org.tak.zeger.enversvalidationplugin.utils.ReflectionUtils;

public class ValidatorInstanceCreator
{
	private final ConnectionProviderInstance connectionProvider;
	private final Map<String, WhitelistEntry> whiteList;
	private final Set<String> auditTablesInDatabase;
	private final Class<?> validatorClass;
	private final List<ValidatorWrapper> validators;

	public ValidatorInstanceCreator(@Nonnull ConnectionProviderInstance connectionProvider, @Nonnull Map<String, WhitelistEntry> whiteList, @Nonnull Set<String> auditTablesInDatabase, @Nonnull Class<?> validatorClass) throws IllegalAccessException, InstantiationException, InvocationTargetException
	{
		this.connectionProvider = connectionProvider;
		this.whiteList = whiteList;
		this.auditTablesInDatabase = auditTablesInDatabase;
		this.validatorClass = validatorClass;
		validators = generateValidators();
	}

	@Nonnull
	private List<ValidatorWrapper> generateValidators() throws IllegalAccessException, InvocationTargetException, InstantiationException
	{
		for (Method method : validatorClass.getMethods())
		{
			final Parameterized parameterized = method.getAnnotation(Parameterized.class);
			if (parameterized != null && Modifier.isStatic(method.getModifiers()))
			{
				try
				{
					final List<Object> methodParameters = provideParameterizedMethodWithParameters(method);

					@SuppressWarnings("unchecked")
					final List<Object[]> generatedData = (List<Object[]>) method.invoke(null, methodParameters.toArray());

					final List<ValidatorWrapper> validatorInstances = new ArrayList<>(generatedData.size());
					for (int index = 0; index < generatedData.size(); index++)
					{
						final Object[] constructorParameters = generatedData.get(index);
						validatorInstances.add(new ParameterizedValidatorWrapper(createValidatorInstance(constructorParameters), constructorParameters, index));
					}
					return validatorInstances;
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
		return Collections.singletonList(new SingleValidatorWrapper(createValidatorInstance(new Object[] {})));
	}

	@Nonnull
	private List<Object> provideParameterizedMethodWithParameters(@Nonnull Method method)
	{
		final List<Object> methodParameters = new ArrayList<>(method.getParameterCount());
		for (Parameter parameter : method.getParameters())
		{
			final ConnectionProvider connectionProviderAnnotation = parameter.getAnnotation(ConnectionProvider.class);
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
		return methodParameters;
	}

	@Nonnull
	public List<ValidatorWrapper> getValidators() throws NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException
	{
		return validators;
	}

	@Nonnull
	private Object createValidatorInstance(@Nonnull Object[] constructorArguments) throws InstantiationException, IllegalAccessException, InvocationTargetException
	{
		final Object newInstance = validatorClass.getConstructors()[0].newInstance(constructorArguments);

		final List<Field> declaredFields = ReflectionUtils.getAllDeclaredFields(validatorClass);
		for (Field declaredField : declaredFields)
		{
			declaredField.setAccessible(true);

			final ConnectionProvider connectionProviderAnnotation = declaredField.getAnnotation(ConnectionProvider.class);
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
}