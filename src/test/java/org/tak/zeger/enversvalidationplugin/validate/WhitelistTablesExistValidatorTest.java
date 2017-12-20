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
import org.tak.zeger.enversvalidationplugin.entities.WhitelistEntry;

@RunWith(PowerMockRunner.class)
public class WhitelistTablesExistValidatorTest
{
	@Mock
	private ConnectionProviderInstance connectionProvider;

	@Test
	public void testGenerateTestDataWithEmptyWhitelist()
	{
		// Given
		final Map<String, WhitelistEntry> whitelist = Collections.emptyMap();

		// When
		final List<Object[]> testData = WhitelistTablesExistValidator.generateTestData(connectionProvider, whitelist);

		// Then
		assertTrue(testData.isEmpty());
	}

	@Test
	public void testGenerateTestDataWithFilledWhitelist()
	{
		// Given
		final WhitelistEntry whitelistEntry1 = new WhitelistEntry("1", "1");
		final WhitelistEntry whitelistEntry2 = new WhitelistEntry("2", "2");
		final WhitelistEntry whitelistEntry3 = new WhitelistEntry("3", "3");

		final Map<String, WhitelistEntry> whitelist = new HashMap<>();
		whitelist.putIfAbsent("1", whitelistEntry1);
		whitelist.putIfAbsent("2", whitelistEntry2);
		whitelist.putIfAbsent("3", whitelistEntry3);

		// When
		final List<Object[]> testData = WhitelistTablesExistValidator.generateTestData(connectionProvider, whitelist);

		// Then
		assertEquals(3, testData.size());
		assertTestDataEqualsWhitelist(whitelistEntry1, testData.get(0));
		assertTestDataEqualsWhitelist(whitelistEntry2, testData.get(1));
		assertTestDataEqualsWhitelist(whitelistEntry3, testData.get(2));
	}

	private void assertTestDataEqualsWhitelist(@Nonnull WhitelistEntry expectedWhitelistEntry, @Nonnull Object[] testDataRow)
	{
		assertEquals(connectionProvider, testDataRow[0]);
		assertEquals(expectedWhitelistEntry.getAuditTableName(), testDataRow[1]);
		assertEquals(expectedWhitelistEntry, testDataRow[2]);
	}
}