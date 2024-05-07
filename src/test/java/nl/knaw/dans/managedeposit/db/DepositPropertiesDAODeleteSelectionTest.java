/*
 * Copyright (C) 2023 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.knaw.dans.managedeposit.db;

import nl.knaw.dans.managedeposit.AbstractDatabaseTest;
import nl.knaw.dans.managedeposit.core.DepositProperties;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class DepositPropertiesDAODeleteSelectionTest extends AbstractDatabaseTest {

    @Test
    public void should_delete_records_for_specified_users() {

        // Create a list of DepositProperties objects and persist them
        var now = OffsetDateTime.now();

        // Create a list of DepositProperties objects and persist them
        var dps = List.of(
            new DepositProperties("Id1", "User1", "Bag1", "State1",
                "Description1", now.plusHours(1), "Location1", 1000L, now.plusHours(2)),
            new DepositProperties("Id2", "User1", "Bag2", "State2",
                "Description2", now.plusMinutes(3), "Location2", 2000L, now.plusMinutes(4)),
            new DepositProperties("Id3", "User2", "Bag3", "State3",
                "Description3", now.plusSeconds(5), "Location3", 3000L, now.plusSeconds(6)),
            new DepositProperties("Id4", "User2", "Bag4", "State4",
                "Description4", now.plusNanos(7), "Location4", 4000L, now.plusNanos(8)),
            new DepositProperties("Id5", "User3", "Bag5", "State5",
                "Description5", now.plusHours(9), "Location5", 5000L, now.plusHours(10)),
            new DepositProperties("Id6", "User3", "Bag6", "State6",
                "Description6", now.plusMinutes(11), "Location6", 6000L, now.plusMinutes(12))
        );
        daoTestExtension.inTransaction(() -> dps.forEach(dp -> dao.create(dp)));

        // Create query parameters with a fixed order
        var queryParameters = new LinkedHashMap<String, List<String>>();
        queryParameters.put("user", List.of("User2", "User3"));

        var deletedCount = daoTestExtension.inTransaction(() ->
            // method under test
            dao.deleteSelection(queryParameters).orElse(0)
        );

        // Assert that the correct number of records were deleted
        assertThat(deletedCount).isEqualTo(4);

        // Assert that the deleted records no longer exists
        assertThat(dao.findSelection(queryParameters)).isEmpty();
    }
}