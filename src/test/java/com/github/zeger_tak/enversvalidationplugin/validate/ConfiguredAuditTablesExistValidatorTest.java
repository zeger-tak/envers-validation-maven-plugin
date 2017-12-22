package com.github.zeger_tak.enversvalidationplugin.validate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import com.github.zeger_tak.enversvalidationplugin.connection.ConnectionProviderInstance;
import com.github.zeger_tak.enversvalidationplugin.entities.AuditTableInformation;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
public class ConfiguredAuditTablesExistValidatorTest
{
	@Mock
	private ConnectionProviderInstance connectionProvider;

	@Test
	public void testGenerateTestDataWithEmptyAuditTableInformationMap()
	{
		// Given
		final Map<String, AuditTableInformation> auditTableInformationMap = Collections.emptyMap();

		// When
		final List<Object[]> testData = ConfiguredAuditTablesExistValidator.generateTestData(connectionProvider, auditTableInformationMap);

		// Then
		assertTrue(testData.isEmpty());
	}

	@Test
	public void testGenerateTestDataWithFilledAuditTableInformationMap()
	{
		// Given
		final AuditTableInformation auditTableInformation1 = new AuditTableInformation("1", "1");
		final AuditTableInformation auditTableInformation2 = new AuditTableInformation("2", "2");
		final AuditTableInformation auditTableInformation3 = new AuditTableInformation("3", "3");

		final Map<String, AuditTableInformation> auditTableInformationMap = new HashMap<>();
		auditTableInformationMap.putIfAbsent("1", auditTableInformation1);
		auditTableInformationMap.putIfAbsent("2", auditTableInformation2);
		auditTableInformationMap.putIfAbsent("3", auditTableInformation3);

		// When
		final List<Object[]> testData = ConfiguredAuditTablesExistValidator.generateTestData(connectionProvider, auditTableInformationMap);

		// Then
		assertEquals(3, testData.size());
		assertTestDataEqualsAuditTableInformation(auditTableInformation1, testData.get(0));
		assertTestDataEqualsAuditTableInformation(auditTableInformation2, testData.get(1));
		assertTestDataEqualsAuditTableInformation(auditTableInformation3, testData.get(2));
	}

	private void assertTestDataEqualsAuditTableInformation(@Nonnull AuditTableInformation expectedAuditTableInformation, @Nonnull Object[] testDataRow)
	{
		assertEquals(connectionProvider, testDataRow[0]);
		assertEquals(expectedAuditTableInformation.getAuditTableName(), testDataRow[1]);
		assertEquals(expectedAuditTableInformation, testDataRow[2]);
	}
}