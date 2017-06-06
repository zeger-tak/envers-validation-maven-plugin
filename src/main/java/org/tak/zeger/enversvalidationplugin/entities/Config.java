package org.tak.zeger.enversvalidationplugin.entities;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import org.tak.zeger.enversvalidationplugin.execution.ValidatorWrapper;

public class Config
{
	private final List<String> packagesToScanForValidators;
	private final Map<String, String> whiteList;
	private final List<String> ignorables;

	public Config(@Nonnull List<String> packagesToScanForValidators, @Nonnull Map<String, String> whiteList, @Nonnull List<String> ignorables)
	{
		this.packagesToScanForValidators = packagesToScanForValidators;
		this.whiteList = whiteList;
		this.ignorables = ignorables;
	}

	@Nonnull
	public List<String> getPackagesToScanForValidators()
	{
		return packagesToScanForValidators;
	}

	@Nonnull
	public Map<String, String> getWhiteList()
	{
		return whiteList;
	}

	public boolean validationShouldBeIgnored(@Nonnull Class<?> validatorClass)
	{
		return ignorables.contains(validatorClass.getSimpleName());
	}

	public boolean validationShouldBeIgnored(@Nonnull ValidatorWrapper validatorWrapper, @Nonnull Method validatorMethod)
	{
		return ignorables.contains(validatorWrapper.getUniqueIdentifier(validatorMethod));
	}
}