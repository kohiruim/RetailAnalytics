SET DATESTYLE TO iso, DMY;
DROP FUNCTION IF EXISTS offer_frequency_visits(first_date TIMESTAMP, last_date TIMESTAMP,
                                               count_transactions INT, max_churn_rate NUMERIC,
                                               max_share_of_discount_transaction NUMERIC,
                                               margin_share double precision) CASCADE;

CREATE OR REPLACE FUNCTION offer_frequency_visits(first_date TIMESTAMP, last_date TIMESTAMP,
                                                  count_transactions INT, max_churn_rate NUMERIC,
                                                  max_share_of_discount_transaction NUMERIC,
                                                  margin_share double precision)
    RETURNS TABLE
            (
                Customer_ID                 INT,
                Start_Date                  TIMESTAMP,
                End_Date                    TIMESTAMP,
                Required_Transactions_Count NUMERIC,
                Group_Name                  VARCHAR,
                Offer_Discount_Depth        NUMERIC
            )
AS
$$
BEGIN
    RETURN QUERY
        WITH required_transactions_count AS (SELECT cv.customer_id,
                                                    ((countdays(last_date - first_date)::double precision /
                                                      Customer_Frequency)::int +
                                                     count_transactions)::numeric AS Required_Transactions_Count
                                             FROM customer_view AS cv),
             group_name AS (SELECT DISTINCT gv.customer_id,
                                            FIRST_VALUE(gs.group_name)
                                            OVER (PARTITION BY gv.customer_id ORDER BY gv.group_affinity_index DESC) AS group_name,
                                            gv.group_minimum_discount * 1.05                                         AS offer_discount_dept
                            FROM groups_view AS gv
                                     JOIN groups_sku gs ON gv.group_id = gs.group_id
                            WHERE gv.group_churn_rate <= max_churn_rate
                              AND gv.group_discount_share < max_share_of_discount_transaction / 100
                              AND gv.group_margin::numeric * margin_share / 100 > gv.group_minimum_discount * 1.05)
        SELECT rtc.customer_id,
               first_date,
               last_date,
               rtc.Required_Transactions_Count,
               gn.group_name,
               gn.offer_discount_dept
        FROM required_Transactions_Count AS rtc
                 JOIN group_name AS gn ON gn.customer_id = rtc.customer_id;
END
$$ LANGUAGE plpgsql;

SELECT *
FROM offer_frequency_visits('2018-08-23 00:47:37.00', '2023-08-23 00:47:37.00', 1, 3, 70, 30);
