package org.tak.zeger.enversvalidationplugin.execution;

import java.util.HashMap;
import java.util.HashSet;
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
import org.tak.zeger.enversvalidationplugin.annotation.TargetPhase;
import org.tak.zeger.enversvalidationplugin.annotation.ValidationType;
import org.tak.zeger.enversvalidationplugin.connection.ConnectionProviderInstance;
import org.tak.zeger.enversvalidationplugin.entities.ValidationResults;
import org.tak.zeger.enversvalidationplugin.entities.WhitelistEntry;
import org.tak.zeger.enversvalidationplugin.utils.ReflectionUtils;

/**
 * This executor is responsible for filtering the whitelist so only audit tables remain which exist in the database and audit a content table which also exists in the database.
 */
public class SetupExecutor extends AbstractExecutor
{
	public SetupExecutor(@Nonnull Log log, @Nonnull List<String> ignorables, @Nonnull ConnectionProviderInstance connectionProvider)
	{
		super(connectionProvider, log, ignorables);
	}

	public void execute(@Nonnull List<String> packagesToScanForValidators, @Nonnull Map<String, WhitelistEntry> providedWhitelist, @Nonnull ValidationResults validationResults)
	{
		final Reflections reflections = new Reflections(new ConfigurationBuilder().setUrls(ReflectionUtils.getPackages(packagesToScanForValidators)).setScanners(new SubTypesScanner(), new FieldAnnotationsScanner(), new TypeAnnotationsScanner()));
		final Set<Class<?>> allValidators = reflections.getTypesAnnotatedWith(ValidationType.class);

		final Map<TargetPhase, Set<Class<?>>> validatorsGroupedByTargetPhase = groupByTargetPhase(allValidators);
		Map<String, WhitelistEntry> whitelist = executeValidators(validatorsGroupedByTargetPhase, TargetPhase.SETUP, providedWhitelist, validationResults);
		whitelist = executeValidators(validatorsGroupedByTargetPhase, TargetPhase.TABLE_STRUCTURE, providedWhitelist, validationResults);
		whitelist = executeValidators(validatorsGroupedByTargetPhase, TargetPhase.CONSTRAINTS, providedWhitelist, validationResults);
		whitelist = executeValidators(validatorsGroupedByTargetPhase, TargetPhase.CONTENT, providedWhitelist, validationResults);
	}

	@Nonnull
	private Map<TargetPhase, Set<Class<?>>> groupByTargetPhase(@Nonnull Set<Class<?>> allValidators)
	{
		final Map<TargetPhase, Set<Class<?>>> validatorsGroupedByTargetPhase = new HashMap<>();
		for (Class<?> validatorClass : allValidators)
		{
			final ValidationType validationType = validatorClass.getAnnotation(ValidationType.class);
			validatorsGroupedByTargetPhase.putIfAbsent(validationType.value(), new HashSet<>());
			validatorsGroupedByTargetPhase.get(validationType.value()).add(validatorClass);
		}
		return validatorsGroupedByTargetPhase;
	}
}
