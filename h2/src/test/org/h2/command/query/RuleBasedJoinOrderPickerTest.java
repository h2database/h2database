package org.h2.command.query;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.h2.engine.Database;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.condition.BooleanTest;
import org.h2.expression.condition.Comparison;
import org.h2.expression.condition.ConditionAndOr;
import org.h2.expression.condition.ConditionAndOrN;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;

public class RuleBasedJoinOrderPickerTest {
    SessionLocal mockSession;
    Database mockDatabase;

    RuleBasedJoinOrderPicker ruleBasedJoinOrderPicker;

    Table customersTable;
    Table locationsTable;
    Table ordersTable;
    Table orderLinesTable;

    ExpressionColumn locationsLocationId;

    ExpressionColumn customersCustomerId;
    ExpressionColumn customersLocationId;

    ExpressionColumn ordersOrderId;
    ExpressionColumn ordersLocationId;
    ExpressionColumn ordersCustomerId;

    ExpressionColumn orderLinesOrderLineId;
    ExpressionColumn orderLinesOrderId;
    ExpressionColumn orderLinesLocationId;
    ExpressionColumn orderLinesCustomerId;

    @BeforeEach
    public void setUp(){
        mockSession = Mockito.mock(SessionLocal.class);
        Mockito.when(mockSession.nextObjectId()).thenReturn(1);

        mockDatabase = Mockito.mock(Database.class);

        // for the purposes of this unit test, we will use four mock tables with
        // multiple relationships between them
        locationsTable = Mockito.mock(Table.class);
        Mockito.when(locationsTable.getName()).thenReturn("locations");
        Mockito.when(locationsTable.getRowCountApproximation(mockSession)).thenReturn(15L);

        customersTable = Mockito.mock(Table.class);
        Mockito.when(customersTable.getName()).thenReturn("customers");
        Mockito.when(customersTable.getRowCountApproximation(mockSession)).thenReturn(50L);

        ordersTable = Mockito.mock(Table.class);
        Mockito.when(ordersTable.getName()).thenReturn("orders");
        Mockito.when(ordersTable.getRowCountApproximation(mockSession)).thenReturn(1000L);

        orderLinesTable = Mockito.mock(Table.class);
        Mockito.when(orderLinesTable.getName()).thenReturn("orderLines");
        Mockito.when(orderLinesTable.getRowCountApproximation(mockSession)).thenReturn(5000L);

        // locations (15 rows)
        //  -> location_id
        locationsLocationId = Mockito.mock(ExpressionColumn.class);
        Mockito.when(locationsLocationId.getTableName()).thenReturn("locations");
        // customers (50 rows)
        //  -> location_id
        customersCustomerId = Mockito.mock(ExpressionColumn.class);
        customersLocationId = Mockito.mock(ExpressionColumn.class);
        Mockito.when(customersCustomerId.getTableName()).thenReturn("customers");
        Mockito.when(customersLocationId.getTableName()).thenReturn("customers");
        // orders (1,000 rows)
        //  -> location_id
        //  -> customer_id
        ordersOrderId = Mockito.mock(ExpressionColumn.class);
        ordersLocationId = Mockito.mock(ExpressionColumn.class);
        ordersCustomerId = Mockito.mock(ExpressionColumn.class);
        Mockito.when(ordersOrderId.getTableName()).thenReturn("orders");
        Mockito.when(ordersLocationId.getTableName()).thenReturn("orders");
        Mockito.when(ordersCustomerId.getTableName()).thenReturn("orders");
        // order_lines (5,000)
        //  -> order_id
        //  -> location_id
        //  -> customer_id
        orderLinesOrderLineId = Mockito.mock(ExpressionColumn.class);
        orderLinesOrderId = Mockito.mock(ExpressionColumn.class);
        orderLinesLocationId = Mockito.mock(ExpressionColumn.class);
        orderLinesCustomerId = Mockito.mock(ExpressionColumn.class);
        Mockito.when(orderLinesOrderLineId.getTableName()).thenReturn("orderLines");
        Mockito.when(orderLinesOrderId.getTableName()).thenReturn("orderLines");
        Mockito.when(orderLinesLocationId.getTableName()).thenReturn("orderLines");
        Mockito.when(orderLinesCustomerId.getTableName()).thenReturn("orderLines");
    }

    @Test
    public void bestOrder_singleTable(){
        TableFilter tableFilter = new TableFilter(mockSession, customersTable, "customers", true, null, 0, null);
        tableFilter.setFullCondition(null);

        List<TableFilter> expectedFilters = List.of(tableFilter);
        TableFilter[] inputFilters = {tableFilter};

        ruleBasedJoinOrderPicker = new RuleBasedJoinOrderPicker(mockSession, inputFilters);
        List<TableFilter> result = Arrays.asList(ruleBasedJoinOrderPicker.bestOrder());

        assertEquals(expectedFilters, result);
    }

    @Test
    public void bestOrder_twoTablesSingleJoin(){
        Expression locationsAndCustomers = new Comparison(
                Comparison.EQUAL,
                locationsLocationId,
                customersLocationId,
                false);

        Expression fullCondition = new ConditionAndOrN(ConditionAndOr.AND,
                List.of(
                        locationsAndCustomers
                )
        );

        TableFilter locationsFilter = new TableFilter(mockSession, locationsTable, "locations", true, null, 0, null);
        locationsFilter.setFullCondition(fullCondition);

        TableFilter customersFilter = new TableFilter(mockSession, customersTable, "customers", true, null, 0, null);
        customersFilter.setFullCondition(fullCondition);

        // locations is smaller so should go first
        List<TableFilter> expectedFilters = List.of(locationsFilter, customersFilter);

        TableFilter[] inputFilters = {customersFilter, locationsFilter};

        ruleBasedJoinOrderPicker = new RuleBasedJoinOrderPicker(mockSession, inputFilters);
        List<TableFilter> result = Arrays.asList(ruleBasedJoinOrderPicker.bestOrder());

        assertEquals(expectedFilters, result);
    }

    @Test
    public void bestOrder_threeTablesMultipleJoins(){
        Expression fullCondition = new ConditionAndOrN(ConditionAndOr.AND,
                List.of(
                        new Comparison(Comparison.EQUAL, locationsLocationId, customersLocationId, false),
                        new Comparison(Comparison.EQUAL, locationsLocationId, ordersLocationId, false),
                        new Comparison(Comparison.EQUAL, customersCustomerId, ordersCustomerId, false)
                )
        );

        TableFilter locationsFilter = new TableFilter(mockSession, locationsTable, "locations", true, null, 0, null);
        locationsFilter.setFullCondition(fullCondition);

        TableFilter customersFilter = new TableFilter(mockSession, customersTable, "customers", true, null, 0, null);
        customersFilter.setFullCondition(fullCondition);

        TableFilter ordersFilter = new TableFilter(mockSession, ordersTable, "orders", true, null, 0, null);
        customersFilter.setFullCondition(fullCondition);

        // size order is locations, customers, orders
        List<TableFilter> expectedFilters = List.of(locationsFilter, customersFilter, ordersFilter);

        TableFilter[] inputFilters = {customersFilter, ordersFilter, locationsFilter};

        ruleBasedJoinOrderPicker = new RuleBasedJoinOrderPicker(mockSession, inputFilters);
        List<TableFilter> result = Arrays.asList(ruleBasedJoinOrderPicker.bestOrder());

        assertEquals(expectedFilters, result);
    }

    @Test
    public void bestOrder_fourTablesMultipleJoins(){
        Expression fullCondition = new ConditionAndOrN(ConditionAndOr.AND,
                List.of(
                        new Comparison(Comparison.EQUAL, locationsLocationId, customersLocationId, false),
                        new Comparison(Comparison.EQUAL, locationsLocationId, ordersLocationId, false),
                        new Comparison(Comparison.EQUAL, customersCustomerId, ordersCustomerId, false),
                        new Comparison(Comparison.EQUAL, orderLinesLocationId, locationsLocationId, false),
                        new Comparison(Comparison.EQUAL, orderLinesOrderId, ordersOrderId, false),
                        new Comparison(Comparison.EQUAL, orderLinesCustomerId, customersCustomerId, false)
                )
        );

        TableFilter locationsFilter = new TableFilter(mockSession, locationsTable, "locations", true, null, 0, null);
        locationsFilter.setFullCondition(fullCondition);

        TableFilter customersFilter = new TableFilter(mockSession, customersTable, "customers", true, null, 0, null);
        customersFilter.setFullCondition(fullCondition);

        TableFilter ordersFilter = new TableFilter(mockSession, ordersTable, "orders", true, null, 0, null);
        ordersFilter.setFullCondition(fullCondition);

        TableFilter orderLinesFilter = new TableFilter(mockSession, orderLinesTable, "orderLines", true, null, 0, null);
        orderLinesFilter.setFullCondition(fullCondition);

        // size order is locations, customers, orders, orderLines
        List<TableFilter> expectedFilters = List.of(locationsFilter, customersFilter, ordersFilter, orderLinesFilter);

        TableFilter[] inputFilters = {orderLinesFilter, customersFilter, ordersFilter, locationsFilter};

        ruleBasedJoinOrderPicker = new RuleBasedJoinOrderPicker(mockSession, inputFilters);
        List<TableFilter> result = Arrays.asList(ruleBasedJoinOrderPicker.bestOrder());

        assertEquals(expectedFilters, result);
    }
}