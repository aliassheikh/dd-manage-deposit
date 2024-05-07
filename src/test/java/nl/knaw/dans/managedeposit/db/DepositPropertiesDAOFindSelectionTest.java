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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import nl.knaw.dans.managedeposit.AbstractDatabaseTest;
import nl.knaw.dans.managedeposit.core.DepositProperties;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.List;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static nl.knaw.dans.managedeposit.TestUtils.captureLog;
import static org.assertj.core.api.Assertions.assertThat;

public class DepositPropertiesDAOFindSelectionTest extends AbstractDatabaseTest {

    @Test
    public void should_return_records_for_specified_users() {

        // Create a list of DepositProperties objects and persist them
        var now = OffsetDateTime.now();
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
                "Description5", null, "Location5", 5000L, null),
            new DepositProperties("Id6", "User3", "Bag6", "State6",
                "Description6", now.plusMinutes(11), "Location6", 6000L, now.plusMinutes(12))
        );
        daoTestExtension.inTransaction(() -> dps.forEach(dp -> dao.create(dp)));

        var sqlLogger = captureLog(Level.DEBUG, "org.hibernate.SQL");
        var valuesLogger = captureLog(Level.TRACE, "org.hibernate.type.descriptor.sql.BasicBinder");

        // Create query parameters with a fixed order
        var queryParameters = new LinkedHashMap<String, List<String>>();
        queryParameters.put("user", List.of("User2", "User3"));

        var results = daoTestExtension.inTransaction(() ->
            // method under test
            dao.findSelection(queryParameters)
        );

        // Assert generated where clause and bound values
        var sqlMessages = sqlLogger.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
        assertThat(sqlMessages.get(0))
            .endsWith(" from deposit_properties depositpro0_ where depositpro0_.depositor=? or depositpro0_.depositor=?");
        var valueMessages = valuesLogger.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
        assertThat(valueMessages).isEqualTo(List.of(
            "binding parameter [1] as [VARCHAR] - [%s]".formatted("User2"),
            "binding parameter [2] as [VARCHAR] - [%s]".formatted("User3")
        ));

        // Assert that the result contains the expected DepositProperties
        assertThat(results)
            .hasSize(4)
            .extracting(DepositProperties::getDepositor)
            .containsOnly("User2", "User3");
        assertThat(results)
            .extracting(DepositProperties::getDepositId)
            .containsExactlyInAnyOrder("Id3", "Id4", "Id5", "Id6");
    }

    @Test
    public void should_return_records_for_specified_user_and_period() {

        // Create a list of DepositProperties objects and persist them
        var now = OffsetDateTime.now();
        var dps = List.of(
            new DepositProperties("Id1", "User1", "Bag1", "State1",
                "Description1", now.minusDays(10), "Location1", 1000L, now.plusHours(2)),
            new DepositProperties("Id2", "User1", "Bag2", "State2",
                "Description2", now.minusDays(5), "Location2", 2000L, now.plusMinutes(4)),
            new DepositProperties("Id3", "User2", "Bag3", "State3",
                "Description3", now.minusDays(3), "Location3", 3000L, now.plusSeconds(6)),
            new DepositProperties("Id4", "User2", "Bag4", "State4",
                "Description4", now.minusDays(1), "Location4", 4000L, now.plusNanos(8)),
            new DepositProperties("Id5", "User2", "Bag5", "State5",
                "Description5", now.plusDays(1), "Location5", 5000L, now.plusHours(10)),
            new DepositProperties("Id6", "User2", "Bag6", "State6",
                "Description6", now.plusDays(3), "Location6", 6000L, now.plusMinutes(12))
        );
        daoTestExtension.inTransaction(() -> dps.forEach(dp -> dao.create(dp)));

        var sqlLogger = captureLog(Level.DEBUG, "org.hibernate.SQL");
        var valuesLogger = captureLog(Level.TRACE, "org.hibernate.type.descriptor.sql.BasicBinder");

        // Create query parameters with a fixed order
        var start1 = now.minusDays(4).format(ISO_LOCAL_DATE);
        var end1 = now.minusDays(2).format(ISO_LOCAL_DATE);
        var start2 = now.format(ISO_LOCAL_DATE);
        var end2 = now.plusDays(2).format(ISO_LOCAL_DATE);
        var queryParameters = new LinkedHashMap<String, List<String>>();
        queryParameters.put("user", List.of("User2"));
        queryParameters.put("startdate", List.of(start1, start2));
        queryParameters.put("enddate", List.of(end1, end2));

        var results = daoTestExtension.inTransaction(() ->
            // method under test
            dao.findSelection(queryParameters)
        );

        // Assert generated where clause and bound values
        var valueMessages = valuesLogger.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
        var sqlMessages = sqlLogger.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
        assertThat(sqlMessages.get(0))
            .endsWith(" where (depositpro0_.deposit_creation_timestamp between ? and ? or depositpro0_.deposit_creation_timestamp between ? and ?) and depositpro0_.depositor=?");
        assertThat(valueMessages).isEqualTo(List.of(
            "binding parameter [1] as [TIMESTAMP] - [%s]".formatted(start1+"T00:00Z"),
            "binding parameter [2] as [TIMESTAMP] - [%s]".formatted(end1+"T00:00Z"),
            "binding parameter [3] as [TIMESTAMP] - [%s]".formatted(start2+"T00:00Z"),
            "binding parameter [4] as [TIMESTAMP] - [%s]".formatted(end2+"T00:00Z"),
            "binding parameter [5] as [VARCHAR] - [%s]".formatted("User2")
            ));

        // Assert that the result contains the expected DepositProperties
        assertThat(results)
            .hasSize(2)
            .extracting(DepositProperties::getDepositor)
            .containsOnly("User2");
        assertThat(results)
            .extracting(DepositProperties::getDepositId)
            .containsExactlyInAnyOrder("Id3", "Id5");
    }

    @Test
    public void should_return_records_for_empty_startdate_and_no_enddate() {

        // Create a list of DepositProperties objects and persist them
        var now = OffsetDateTime.now();
        var dps = List.of(
            new DepositProperties("Id3", "User2", "Bag3", "State3",
                "Description3", null, "Location3", 3000L, null),
            new DepositProperties("Id4", "User2", "Bag4", "State4",
                "Description4", now.minusDays(1), "Location4", 4000L, now.plusNanos(8))
        );
        daoTestExtension.inTransaction(() -> dps.forEach(dp -> dao.create(dp)));

        var sqlLogger = captureLog(Level.DEBUG, "org.hibernate.SQL");
        var valuesLogger = captureLog(Level.TRACE, "org.hibernate.type.descriptor.sql.BasicBinder");

        // Create query parameters with a fixed order
        var queryParameters = new LinkedHashMap<String, List<String>>();
        queryParameters.put("startdate", List.of());

        var results = daoTestExtension.inTransaction(() ->
            // method under test
            dao.findSelection(queryParameters)
        );

        // Assert generated where clause and bound values
        var sqlMessages = sqlLogger.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
        assertThat(sqlMessages.get(0))
            .endsWith(" from deposit_properties depositpro0_ where depositpro0_.deposit_creation_timestamp is null");
        var valueMessages = valuesLogger.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
        assertThat(valueMessages).isEmpty();

        // Assert that the result contains the expected DepositProperties
        assertThat(results)
            .hasSize(1)
            .extracting(DepositProperties::getDepositId)
            .containsExactlyInAnyOrder("Id3");
    }
}