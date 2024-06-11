DROP VIEW IF EXISTS Customer_view CASCADE;
DROP TABLE IF EXISTS table_segments CASCADE;
DROP FUNCTION IF EXISTS countDays(DATE interval) CASCADE;

CREATE OR REPLACE FUNCTION countDays(date interval)
    RETURNS numeric AS
$$
DECLARE
    counterDays    numeric := EXTRACT(DAY FROM date);
    counterHours   numeric := EXTRACT(HOUR FROM date) / 24;
    counterMinutes numeric := EXTRACT(MINUTE FROM date) / 60 / 24;
    counterSeconds numeric := EXTRACT(SECOND FROM date) / 60 / 24 / 60;
    counterYears   numeric := EXTRACT(YEAR FROM date);
BEGIN
    IF (counterYears % 4 = 0)
    THEN
        counterYears = counterYears * 366;
    ELSE
        counterYears = counterYears * 365;
    END IF;
    counterDays = counterYears + counterDays + counterHours + counterMinutes + counterSeconds;
    RETURN counterDays;
END
$$ LANGUAGE plpgsql;

CREATE TABLE table_segments
(
    Segment           int,
    Check_Segment     varchar,
    Frequency_Segment varchar,
    Churn_Segment     varchar
);

INSERT INTO table_segments
VALUES (1, 'Low', 'Rarely', 'Low'),
       (2, 'Low', 'Rarely', 'Medium'),
       (3, 'Low', 'Rarely', 'High'),
       (4, 'Low', 'Occasionally', 'Low'),
       (5, 'Low', 'Occasionally', 'Medium'),
       (6, 'Low', 'Occasionally', 'High'),
       (7, 'Low', 'Often', 'Low'),
       (8, 'Low', 'Often', 'Medium'),
       (9, 'Low', 'Often', 'High'),
       (10, 'Medium', 'Rarely', 'Low'),
       (11, 'Medium', 'Rarely', 'Medium'),
       (12, 'Medium', 'Rarely', 'High'),
       (13, 'Medium', 'Occasionally', 'Low'),
       (14, 'Medium', 'Occasionally', 'Medium'),
       (15, 'Medium', 'Occasionally', 'High'),
       (16, 'Medium', 'Often', 'Low'),
       (17, 'Medium', 'Often', 'Medium'),
       (18, 'Medium', 'Often', 'High'),
       (19, 'High', 'Rarely', 'Low'),
       (20, 'High', 'Rarely', 'Medium'),
       (21, 'High', 'Rarely', 'High'),
       (22, 'High', 'Occasionally', 'Low'),
       (23, 'High', 'Occasionally', 'Medium'),
       (24, 'High', 'Occasionally', 'High'),
       (25, 'High', 'Often', 'Low'),
       (26, 'High', 'Often', 'Medium'),
       (27, 'High', 'Often', 'High');

CREATE VIEW Customer_view AS
WITH customers_data AS (SELECT customer_id,
                               cards.customer_card_id,
                               transaction_id,
                               transaction_sum,
                               transaction_datetime,
                               transaction_store_id
                        FROM cards
                                 JOIN transactions ON cards.customer_card_id = transactions.customer_card_id
                        WHERE transaction_datetime <= (SELECT MAX(Analysis_Formation)
                                                       FROM date_analysis_formation
                                                       ORDER BY customer_id)),
     for_average_check AS (SELECT MIN(customer_id)                                    AS customer_id,
                                  ROUND(SUM(transaction_sum) / COUNT(*)::numeric, 2)  AS Customer_Average_Check,
                                  ROUND(countDays(AGE(MAX(transaction_datetime), MIN(transaction_datetime))) / COUNT(*),
                                        6)                                            AS Customer_Frequency,
                                  ROUND(countDays(AGE((SELECT MAX(Analysis_Formation) FROM date_analysis_formation),
                                                      MAX(transaction_datetime))), 6) AS Customer_Inactive_Period
                           FROM customers_data
                           GROUP BY customer_id
                           ORDER BY Customer_Average_Check DESC, Customer_Frequency),
     all_factors_data AS (SELECT customer_id,
                                 Customer_Average_Check,
                                 Customer_Frequency,
                                 Customer_Inactive_Period,
                                 ROUND(Customer_Inactive_Period / Customer_Frequency, 6) AS Customer_Churn_Rate
                          FROM for_average_check
                          ORDER BY Customer_Average_Check DESC, Customer_Frequency),
     all_segments_by_factors AS (SELECT customer_id,
                                        (CASE
                                             WHEN ROW_NUMBER() OVER () <=
                                                  ROUND((SELECT COUNT(*) FROM all_factors_data) * 0.1, 0) THEN 'High'
                                             WHEN ROW_NUMBER() OVER () <=
                                                  ROUND((SELECT COUNT(*) FROM all_factors_data) * 0.35, 0) THEN 'Medium'
                                             ELSE 'Low'
                                            END) AS Customer_Average_Check_Segment,
                                        (CASE
                                             WHEN ROW_NUMBER() OVER () <=
                                                  ROUND((SELECT COUNT(*) FROM all_factors_data) * 0.1, 0) THEN 'Often'
                                             WHEN ROW_NUMBER() OVER () <=
                                                  ROUND((SELECT COUNT(*) FROM all_factors_data) * 0.35, 0)
                                                 THEN 'Occasionally'
                                             ELSE 'Rarely'
                                            END) AS Customer_Frequency_Segment,
                                        (CASE
                                             WHEN Customer_Inactive_Period / Customer_Frequency < 2 THEN 'Low'
                                             WHEN Customer_Inactive_Period / Customer_Frequency < 5 THEN 'Occasionally'
                                             ELSE 'High'
                                            END) AS Customer_Churn_Segment
                                 FROM all_factors_data),
     customer_segment AS (SELECT customer_id,
                                 (SELECT segment
                                  FROM table_segments
                                  WHERE Customer_Average_Check_Segment = table_segments.Check_Segment
                                    AND Customer_Frequency_Segment = table_segments.Frequency_Segment
                                    AND Customer_Churn_Segment = table_segments.Churn_Segment) AS Customer_Segment
                          FROM all_segments_by_factors),
     customer_stores AS (SELECT c.customer_id, s.transaction_store_id, COUNT(DISTINCT t.transaction_id) AS tr_store
                         FROM cards c
                                  JOIN transactions t ON c.customer_card_id = t.customer_card_id
                                  JOIN stores s ON t.transaction_store_id = s.transaction_store_id
                         WHERE t.transaction_datetime <= (SELECT MAX(Analysis_Formation) FROM date_analysis_formation)
                         GROUP BY 1, 2
                         ORDER BY 1),
     share_transaction_store AS (SELECT customer_id,
                                        transaction_store_id,
                                        ROUND(tr_store / (SUM(tr_store) OVER (PARTITION BY transaction_store_id)),
                                              6) AS share
                                 FROM customer_stores
                                 ORDER BY 1, 2),
     share_transaction_store_rank AS (SELECT customer_id,
                                             transaction_store_id,
                                             share,
                                             RANK() OVER (PARTITION BY customer_id ORDER BY share DESC) AS rank
                                      FROM share_transaction_store),
     share_transaction_rank_count AS (SELECT customer_id,
                                             COUNT(share)              AS share_count,
                                             MAX(transaction_store_id) AS store_id
                                      FROM share_transaction_store_rank
                                      WHERE rank = 1
                                      GROUP BY customer_id),
     for_last_transaction AS (SELECT DISTINCT t.transaction_datetime,
                                              customer_id,
                                              c.customer_card_id,
                                              s.transaction_store_id
                              FROM cards AS c
                                       JOIN transactions t ON c.customer_card_id = t.customer_card_id
                                       JOIN stores s ON t.transaction_store_id = s.transaction_store_id
                              ORDER BY 2, 1 DESC),
     last_3_transaction AS (SELECT t_groups.customer_id, transaction_store_id, transaction_datetime
                            FROM (SELECT DISTINCT customer_id FROM for_last_transaction) t_groups
                                     JOIN LATERAL (SELECT *
                                                   FROM for_last_transaction
                                                   WHERE for_last_transaction.customer_id = t_groups.customer_id
                                                   LIMIT 3) t_limited ON TRUE
                                     JOIN cards ON cards.customer_card_id = t_groups.customer_id
                            ORDER BY 1, 2 DESC),
     count_last_3_transaction AS (SELECT customer_id, COUNT(transaction_store_id) AS count, transaction_store_id
                                  FROM (SELECT customer_id,
                                               transaction_store_id,
                                               transaction_datetime,
                                               RANK() OVER (PARTITION BY customer_id
                                                   ORDER BY customer_id, transaction_datetime DESC,
                                                       transaction_store_id) AS rank
                                        FROM customers_data
                                        ORDER BY customer_id, transaction_store_id DESC) AS w
                                  WHERE rank < 4
                                  GROUP BY customer_id, transaction_store_id
                                  ORDER BY customer_id),
     primary_store AS (SELECT DISTINCT count_last_3_transaction.customer_id,
                                       (CASE
                                            WHEN count_last_3_transaction.count = 3
                                                THEN count_last_3_transaction.transaction_store_id
                                            WHEN share_transaction_rank_count.share_count = 1
                                                THEN share_transaction_rank_count.store_id
                                            ELSE last_3_transaction.transaction_store_id
                                           END) AS Customer_Primary_Store
                       FROM count_last_3_transaction
                                JOIN share_transaction_rank_count
                                     ON count_last_3_transaction.customer_id = share_transaction_rank_count.customer_id
                                JOIN last_3_transaction
                                     ON count_last_3_transaction.customer_id = last_3_transaction.customer_id),
     full_table AS (SELECT all_factors_data.customer_id,
                           Customer_Average_Check,
                           Customer_Average_Check_Segment,
                           Customer_Frequency,
                           Customer_Frequency_Segment,
                           Customer_Inactive_Period,
                           Customer_Churn_Rate,
                           Customer_Churn_Segment,
                           customer_segment,
                           Customer_Primary_Store
                    FROM all_factors_data
                             JOIN all_segments_by_factors
                                  ON all_segments_by_factors.customer_id = all_factors_data.customer_id
                             JOIN customer_segment ON customer_segment.customer_id = all_factors_data.customer_id
                             JOIN primary_store ON primary_store.customer_id = all_factors_data.customer_id
                    ORDER BY 1)
SELECT *
FROM full_table;

SELECT *
FROM Customer_view;
