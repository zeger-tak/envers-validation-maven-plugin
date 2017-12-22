package org.tak.zeger.enversvalidationplugin.validate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.modules.junit4.PowerMockRunner;
import org.tak.zeger.enversvalidationplugin.connection.ConnectionProviderInstance;
import org.tak.zeger.enversvalidationplugin.entities.AuditTableInformation;

@RunWith(PowerMockRunner.class)
public class WhitelistTablesExistValidatorTest
{
	@Mock
	private ConnectionProviderInstance connectionProvider;

	@Test
	public void testGenerateTestDataWithEmptyWhitelist()
	{
		// Given
		final Map<String, AuditTableInformation> whitelist = Collections.emptyMap();

		// When
		final List<Object[]> testData = WhitelistTablesExistValidator.generateTestData(connectionProvider, whitelist);

		// Then
		assertTrue(testData.isEmpty());
	}

	@Test
	public void testGenerateTestDataWithFilledWhitelist()
	{
		// Given
		final AuditTableInformation auditTableInformation1 = new AuditTableInformation("1", "1");
		final AuditTableInformation auditTableInformation2 = new AuditTableInformation("2", "2");
		final AuditTableInformation auditTableInformation3 = new AuditTableInformation("3", "3");

		final Map<String, AuditTableInformation> whitelist = new HashMap<>();
		whitelist.putIfAbsent("1", auditTableInformation1);
		whitelist.putIfAbsent("2", auditTableInformation2);
		whitelist.putIfAbsent("3", auditTableInformation3);

		// When
		final List<Object[]> testData = WhitelistTablesExistValidator.generateTestData(connectionProvider, whitelist);

		// Then
		assertEquals(3, testData.size());
		assertTestDataEqualsWhitelist(auditTableInformation1, testData.get(0));
		assertTestDataEqualsWhitelist(auditTableInformation2, testData.get(1));
		assertTestDataEqualsWhitelist(auditTableInformation3, testData.get(2));
	}

	private void assertTestDataEqualsWhitelist(@Nonnull AuditTableInformation expectedAuditTableInformation, @Nonnull Object[] testDataRow)
	{
		assertEquals(connectionProvider, testDataRow[0]);
		assertEquals(expectedAuditTableInformation.getAuditTableName(), testDataRow[1]);
		assertEquals(expectedAuditTableInformation, testDataRow[2]);
	}
}