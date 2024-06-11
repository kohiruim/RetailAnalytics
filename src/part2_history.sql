DROP VIEW IF EXISTS purchase_history_view CASCADE;

CREATE OR REPLACE VIEW purchase_history_view AS
WITH purchase AS (SELECT transactions.transaction_id,
                         c.customer_id,
                         transaction_datetime,
                         group_id,
                         sku_purchase_price,
                         sku_amount,
                         sku_sum,
                         sku_sum_paid
                  FROM transactions
                           JOIN cards c ON c.customer_card_id = transactions.customer_card_id
                           JOIN personal_data pd ON pd.customer_id = c.customer_id
                           JOIN checks c2 ON transactions.transaction_id = c2.transaction_id
                           JOIN sku s ON c2.sku_id = s.sku_id
                           JOIN stores s2 ON s.sku_id = s2.sku_id
                  WHERE transaction_datetime < (SELECT analysis_formation FROM date_analysis_formation))
SELECT DISTINCT customer_id,
                transaction_id,
                transaction_datetime,
                group_id,
                SUM(sku_purchase_price * sku_amount)
                OVER (PARTITION BY customer_id, transaction_id, transaction_datetime, group_id) AS Group_Cost,
                SUM(sku_sum)
                OVER (PARTITION BY customer_id, transaction_id, transaction_datetime, group_id) AS Group_Summ,
                SUM(sku_sum_paid)
                OVER (PARTITION BY customer_id, transaction_id, transaction_datetime, group_id) AS Group_Summ_Paid
FROM purchase;

SELECT *
FROM purchase_history_view
ORDER BY 1, 2;
