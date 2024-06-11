DROP VIEW IF EXISTS Groups_View;
DROP FUNCTION IF EXISTS methods_for_margin() CASCADE;

-- method 1 - метода расчета маржи по периоду
-- method 2 - метод расчета маржи по количеству транзакций
-- count - количество транзакций для метода расчета маржи по количеству транзакций
--          или количество дней от даты формирования анализа для расссчета маржи по периоду

CREATE OR REPLACE FUNCTION methods_for_margin(method INT DEFAULT 1, count INT DEFAULT 10000)
    RETURNS TABLE
            (
                customer_id  int,
                group_id     int,
                group_margin double precision
            )
AS
$$
BEGIN
    IF (method = 1)
    THEN
        RETURN QUERY (SELECT data_for_margin.customer_id, data_for_margin.group_id, SUM(data_for_margin.margin)
                      FROM (SELECT ph.customer_id,
                                   ph.group_id,
                                   (group_summ_paid - group_cost)::double precision AS margin
                            FROM purchase_history_view AS ph
                            WHERE transaction_datetime <= (SELECT analysis_formation FROM date_analysis_formation)
                              AND transaction_datetime >=
                                  (SELECT ((SELECT analysis_formation FROM date_analysis_formation) -
                                           CAST(count || ' days' AS interval)))
                            ORDER BY transaction_datetime DESC) data_for_margin
                      GROUP BY data_for_margin.customer_id, data_for_margin.group_id);
    ELSIF (method = 2)
    THEN
        RETURN QUERY (SELECT data_for_margin.customer_id, data_for_margin.group_id, SUM(data_for_margin.margin)
                      FROM (SELECT ph.customer_id,
                                   ph.group_id,
                                   (group_summ_paid - group_cost)::double precision AS margin
                            FROM purchase_history_view AS ph
                            WHERE transaction_datetime <= (SELECT analysis_formation FROM date_analysis_formation)
                            ORDER BY transaction_datetime DESC
                            LIMIT count) data_for_margin
                      GROUP BY data_for_margin.customer_id, data_for_margin.group_id);
    ELSE
        RAISE EXCEPTION 'Либо такого метода не существует, либо проблема с аргументами';
    END IF;
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE VIEW Groups_View AS
WITH groups AS (SELECT DISTINCT customer_id, group_id
                FROM (SELECT cards.customer_id, sku_id
                      FROM cards
                               JOIN personal_data pd ON cards.customer_id = pd.customer_id
                               JOIN transactions t ON cards.customer_card_id = t.customer_card_id
                               JOIN checks c ON t.transaction_id = c.transaction_id
                      WHERE transaction_datetime < (SELECT analysis_formation FROM date_analysis_formation)) t
                         JOIN sku ON t.sku_id = sku.sku_id),
     for_affinity_index AS (SELECT ph.customer_id,
                                   ph.group_id,
                                   (SELECT Group_Purchase
                                    FROM periods_view
                                    WHERE periods_view.customer_id = ph.customer_id
                                      AND periods_view.group_id = ph.group_id)::double precision              AS Group_Purchase,
                                   COUNT(transaction_id::double precision) OVER (PARTITION BY ph.customer_id) AS count
                            FROM purchase_history_view AS ph
                                     JOIN periods_view pv
                                          ON ph.customer_id = pv.customer_id AND ph.group_id = pv.group_id
                            WHERE transaction_datetime >= first_group_purchase_date
                              AND transaction_datetime <= last_group_purchase_date),
     affinity_index AS (SELECT customer_id, group_id, AVG(Group_Purchase / count) AS Group_Affinity_Index
                        FROM for_affinity_index
                        GROUP BY customer_id, group_id),
     churn_rate AS (SELECT ph.customer_id,
                           ph.group_id,
                           ROUND(countdays((SELECT analysis_formation FROM date_analysis_formation) -
                                           MAX(ph.transaction_datetime)) / pv.group_frequency, 8) AS Group_Churn_Rate
                    FROM purchase_history_view AS ph
                             JOIN periods_view pv ON ph.customer_id = pv.customer_id AND ph.group_id = pv.group_id
                    GROUP BY ph.customer_id, ph.group_id, pv.group_frequency),
     for_stability_index AS (SELECT ph.customer_id,
                                    ph.group_id,
                                    ABS(countDays(ph.transaction_datetime - LAG(ph.transaction_datetime, 1)
                                                                            OVER (PARTITION BY ph.customer_id, ph.group_id ORDER BY ph.transaction_datetime)) -
                                        group_frequency) / group_frequency AS stability_index
                             FROM purchase_history_view AS ph
                                      JOIN periods_view ON ph.customer_id = periods_view.customer_id AND
                                                           ph.group_id = periods_view.group_id),
     for_margin AS (SELECT * FROM methods_for_margin()),
     stability_index AS (SELECT customer_id, group_id, COALESCE(AVG(stability_index), 0) AS Group_Stability_Index
                         FROM for_stability_index
                         GROUP BY customer_id, group_id
                         ORDER BY customer_id, group_id),
     discount_share AS (SELECT DISTINCT ph.customer_id,
                                        ph.group_id,
                                        (COUNT(c2.transaction_id)
                                         FILTER (WHERE sku_discount > 0) OVER (PARTITION BY ph.customer_id, ph.group_id)::double precision /
                                         pv.group_purchase) AS Group_Discount_Share
                        FROM purchase_history_view AS ph
                                 JOIN checks c2 ON ph.transaction_id = c2.transaction_id
                                 JOIN periods_view pv ON ph.customer_id = pv.customer_id AND ph.group_id = pv.group_id
                        ORDER BY ph.customer_id, ph.group_id),
     minimum_discount AS (SELECT customer_id, group_id, MIN(group_min_discount) AS Group_Minimum_Discount
                          FROM periods_view
                          WHERE group_min_discount != 0
                          GROUP BY customer_id, group_id),
     average_discount AS (SELECT customer_id,
                                 group_id,
                                 (SUM(group_summ_paid) / SUM(group_summ)) AS Group_Average_Discount
                          FROM purchase_history_view
                          GROUP BY customer_id, group_id
                          ORDER BY customer_id, group_id)
SELECT groups.customer_id,
       groups.group_id,
       Group_Affinity_Index,
       Group_Churn_Rate,
       Group_Stability_Index,
       Group_Margin,
       Group_Discount_Share,
       COALESCE(Group_Minimum_Discount, 0) AS Group_Minimum_Discount,
       Group_Average_Discount
FROM groups
         JOIN affinity_index
              ON groups.customer_id = affinity_index.customer_id AND groups.group_id = affinity_index.group_id
         JOIN churn_rate ON groups.customer_id = churn_rate.customer_id AND groups.group_id = churn_rate.group_id
         JOIN stability_index
              ON groups.customer_id = stability_index.customer_id AND groups.group_id = stability_index.group_id
         JOIN for_margin ON groups.customer_id = for_margin.customer_id AND groups.group_id = for_margin.group_id
         JOIN discount_share
              ON groups.customer_id = discount_share.customer_id AND groups.group_id = discount_share.group_id
         LEFT JOIN minimum_discount
                   ON groups.customer_id = minimum_discount.customer_id AND groups.group_id = minimum_discount.group_id
         JOIN average_discount
              ON groups.customer_id = average_discount.customer_id AND groups.group_id = average_discount.group_id
ORDER BY 1, 2;

SELECT *
FROM Groups_View;
