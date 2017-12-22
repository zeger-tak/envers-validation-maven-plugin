package com.github.zeger_tak.enversvalidationplugin.execution;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.github.zeger_tak.enversvalidationplugin.annotation.TargetPhase;
import com.github.zeger_tak.enversvalidationplugin.annotation.ValidationType;
import com.github.zeger_tak.enversvalidationplugin.connection.ConnectionProviderInstance;
import com.github.zeger_tak.enversvalidationplugin.entities.AuditTableInformation;
import com.github.zeger_tak.enversvalidationplugin.entities.ValidationResults;
import com.github.zeger_tak.enversvalidationplugin.utils.ReflectionUtils;
import org.apache.maven.plugin.logging.Log;
import org.reflections.Reflections;
import org.reflections.scanners.FieldAnnotationsScanner;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ConfigurationBuilder;

public class SetupExecutor extends AbstractExecutor
{
	public SetupExecutor(@Nonnull Log log, @Nonnull List<String> ignorables, @Nonnull ConnectionProviderInstance connectionProvider)
	{
		super(connectionProvider, log, ignorables);
	}

	public void execute(@Nonnull List<String> packagesToScanForValidators, @Nonnull Map<String, AuditTableInformation> providedAuditTableInformationMap, @Nonnull ValidationResults validationResults)
	{
		final Reflections reflections = new Reflections(new ConfigurationBuilder().setUrls(ReflectionUtils.getPackages(packagesToScanForValidators)).setScanners(new SubTypesScanner(), new FieldAnnotationsScanner(), new TypeAnnotationsScanner()));
		final Set<Class<?>> allValidators = reflections.getTypesAnnotatedWith(ValidationType.class);

		final Map<TargetPhase, Set<Class<?>>> validatorsGroupedByTargetPhase = groupByTargetPhase(allValidators);
		Map<String, AuditTableInformation> auditTableInformationMap = executeValidators(validatorsGroupedByTargetPhase, TargetPhase.SETUP, providedAuditTableInformationMap, validationResults);
		auditTableInformationMap = executeValidators(validatorsGroupedByTargetPhase, TargetPhase.TABLE_STRUCTURE, auditTableInformationMap, validationResults);
		auditTableInformationMap = executeValidators(validatorsGroupedByTargetPhase, TargetPhase.CONSTRAINTS, auditTableInformationMap, validationResults);
		auditTableInformationMap = executeValidators(validatorsGroupedByTargetPhase, TargetPhase.CONTENT, auditTableInformationMap, validationResults);
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
