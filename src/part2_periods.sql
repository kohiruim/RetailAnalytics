DROP VIEW IF EXISTS Periods_View CASCADE;

CREATE OR REPLACE VIEW Periods_View AS
WITH data AS (SELECT customer_id,
                     group_id,
                     transaction_datetime,
                     purchase_history_view.transaction_id,
                     COALESCE(ROUND(MIN(sku_discount / sku_sum) OVER (PARTITION BY customer_id, group_id), 6),
                              0) AS Group_Min_Discount
              FROM purchase_history_view
                       JOIN checks c ON purchase_history_view.transaction_id = c.transaction_id
              WHERE transaction_datetime <= (SELECT analysis_formation FROM date_analysis_formation)),
     group_purchase AS (SELECT customer_id,
                               group_id,
                               MIN(transaction_datetime) AS First_Group_Purchase_Date,
                               MAX(transaction_datetime) AS Last_Group_Purchase_Date,
                               COUNT(*)                  AS Group_Purchase,
                               ROUND((countDays(MAX(transaction_datetime) - MIN(transaction_datetime)) + 1) / COUNT(*),
                                     6)                  AS Group_Frequency,
                               Group_Min_Discount
                        FROM data
                                 JOIN checks c ON data.transaction_id = c.transaction_id
                        GROUP BY customer_id, group_id, Group_Min_Discount
                        ORDER BY customer_id, group_id)
SELECT *
FROM group_purchase;

SELECT *
FROM Periods_View;
