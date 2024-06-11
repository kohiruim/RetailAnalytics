CREATE OR REPLACE FUNCTION offer_margin_growth(count_groups int, max_churn_rate numeric, max_stability_index numeric,
                                               max_share_SKU numeric, max_share_margin numeric)
    RETURNS TABLE
            (
                Customer_ID          int,
                SKU_Name             varchar,
                Offer_Discount_Depth numeric
            )
AS
$$
BEGIN
    RETURN QUERY
        WITH data AS (SELECT DISTINCT gv.customer_id,
                                      sku.sku_name,
                                      gv.group_affinity_index,
                                      gv.Group_Churn_Rate,
                                      gv.Group_Stability_Index,
                                      MAX(stores.sku_retail_price - stores.sku_purchase_price)
                                      OVER (PARTITION BY gv.customer_id, gv.group_id, sku.sku_id),
                                      ROUND(((COUNT(stores.transaction_store_id) OVER (PARTITION BY sku.sku_id)::numeric
                                          / (COUNT(stores.transaction_store_id) OVER (PARTITION BY gv.group_id)))),
                                            6)                                                             AS share_sku_group,
                                      (CASE
                                           WHEN ((max_share_margin / 100 *
                                                  (sku_retail_price - sku_purchase_price)::numeric /
                                                  sku_retail_price) <= group_minimum_discount * 1.05)
                                               THEN CEIL(group_minimum_discount * 100 / 5.0) * 5
                                          END)                                                             AS offer_discount_depth,
                                      DENSE_RANK() OVER (PARTITION BY gv.customer_id ORDER BY gv.group_id) AS ranks
                      FROM groups_view gv
                               JOIN cards ON cards.customer_id = gv.customer_id
                               JOIN sku ON gv.group_id = sku.group_id
                               JOIN stores ON sku.sku_id = stores.sku_id
                      WHERE Group_Churn_Rate <= max_churn_rate
                        AND Group_Stability_Index < max_stability_index)
        SELECT d.customer_id, d.sku_name, ROUND(d.offer_discount_depth)
        FROM data d
        WHERE d.offer_discount_depth IS NOT NULL
          AND max_share_SKU <= d.share_sku_group * 100
          AND count_groups >= ranks;
END
$$ LANGUAGE plpgsql;

SELECT *
FROM offer_margin_growth(100, 100, 100, 2, 10);
