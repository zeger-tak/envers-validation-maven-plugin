package org.tak.zeger.enversvalidationplugin.utils;

import java.lang.reflect.Field;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.annotation.Nonnull;

import org.reflections.util.ClasspathHelper;

public final class ReflectionUtils
{
	private ReflectionUtils()
	{
	}

	@Nonnull
	public static List<Field> getAllDeclaredFields(@Nonnull final Class<?> clazz)
	{
		final List<Field> result;

		final List<Field> declaredFields = Arrays.asList(clazz.getDeclaredFields());

		final Class<?> superclass = clazz.getSuperclass();
		if (superclass != null)
		{
			final List<Field> superclassDeclaredFields = getAllDeclaredFields(superclass);
			result = new ArrayList<Field>(declaredFields.size() + superclassDeclaredFields.size());
			result.addAll(superclassDeclaredFields);
			result.addAll(declaredFields);
		}
		else
		{
			result = new ArrayList<Field>(declaredFields);
		}

		return result;
	}

	@Nonnull
	public static List<URL> getPackages(@Nonnull List<String> packagesToScanForValidators)
	{
		final List<URL> packages = new ArrayList<>(packagesToScanForValidators.size());
		for (String packagesToScanForValidator : packagesToScanForValidators)
		{
			packages.addAll(ClasspathHelper.forPackage(packagesToScanForValidator));
		}
		return packages;
	}
}