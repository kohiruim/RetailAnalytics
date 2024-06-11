DROP FUNCTION IF EXISTS get_offers(integer, date, date, integer, numeric, numeric, numeric, numeric);
CREATE OR REPLACE FUNCTION get_offers(method int, date_first date, date_last date, quantity int,
                                      multiplier numeric, max_churn_index numeric, max_transactions_share numeric,
                                      max_margin_share numeric)
    RETURNS TABLE
            (
                Customer_ID            int,
                Required_Check_Measure numeric,
                Group_Name             varchar,
                Offer_Discount_Depth   numeric
            )

AS
$$
BEGIN
    IF (method NOT IN (1, 2)) THEN
        RAISE EXCEPTION 'Method must be 1 or 2';
    END IF;
    IF (method = 1) THEN
        RETURN QUERY
            SELECT t1.Customer_ID,
                   (t1.Current_average_check * multiplier) AS Required_Check_Measure,
                   gs.group_name,
                   t2.Offer_Discount_Depth
            FROM get_average_check_date(date_first, date_last) t1
                     JOIN group_determination(max_churn_index, max_transactions_share, max_margin_share) t2
                          ON t1.Customer_ID = t2.Customer_ID
                     JOIN groups_sku gs ON gs.group_id = t2.Group_ID;
    ELSIF (method = 2) THEN
        RETURN QUERY
            SELECT t1.Customer_ID,
                   (t1.Current_average_check * multiplier) AS Required_Check_Measure,
                   gs.group_name,
                   t2.Offer_Discount_Depth
            FROM get_average_check_transactions(quantity) t1
                     JOIN group_determination(max_churn_index, max_transactions_share, max_margin_share) t2
                          ON t1.Customer_ID = t2.Customer_ID
                     JOIN groups_sku gs ON gs.group_id = t2.Group_ID;
    END IF;
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS get_average_check_date(date, date);
CREATE OR REPLACE FUNCTION get_average_check_date(date_first date, date_last date)
    RETURNS TABLE
            (
                Customer_ID           int,
                Current_average_check numeric
            )
AS
$$
BEGIN
    IF (date_first < get_first_transactions_date()) THEN
        date_first = get_first_transactions_date();
    ELSIF (date_last > get_last_transactions_date()) THEN
        date_last = get_last_transactions_date();
    END IF;

    RETURN QUERY
        SELECT c.customer_card_id, AVG(t.transaction_sum) AS current_transactions_avg
        FROM cards c
                 JOIN transactions t ON c.customer_card_id = t.customer_card_id
        WHERE transaction_datetime BETWEEN date_last AND date_last
        GROUP BY 1;
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS get_first_transactions_date();
CREATE OR REPLACE FUNCTION get_first_transactions_date()
    RETURNS SETOF date
AS
$$
BEGIN
    RETURN QUERY
        SELECT transaction_datetime::date
        FROM transactions
        ORDER BY transaction_datetime
        LIMIT 1;
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS get_last_transactions_date();
CREATE OR REPLACE FUNCTION get_last_transactions_date()
    RETURNS SETOF date
AS
$$
BEGIN
    RETURN QUERY
        SELECT transaction_datetime::date
        FROM transactions
        ORDER BY transaction_datetime DESC
        LIMIT 1;
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS get_average_check_transactions(int);
CREATE OR REPLACE FUNCTION get_average_check_transactions(quantity int)
    RETURNS TABLE
            (
                Customer_ID           int,
                Current_average_check numeric
            )
AS
$$
BEGIN
    RETURN QUERY
        SELECT customer_card_id, AVG(transaction_sum)
        FROM (SELECT c.customer_card_id,
                     transaction_sum,
                     transaction_datetime,
                     ROW_NUMBER() OVER (PARTITION BY c.customer_card_id ORDER BY transaction_datetime DESC) AS number
              FROM cards c
                       JOIN transactions t ON c.customer_card_id = t.customer_card_id) t
        WHERE number <= quantity
        GROUP BY 1;
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS get_sorted_groups();
CREATE OR REPLACE FUNCTION get_sorted_groups()
    RETURNS TABLE
            (
                customer_id            int,
                group_id               int,
                group_affinity_index   double precision,
                group_churn_rate       numeric,
                group_discount_share   double precision,
                group_minimum_discount numeric,
                group_margin_average   double precision
            )
AS
$$
BEGIN
    RETURN QUERY
        WITH groups AS
                 (SELECT *,
                         RANK() OVER (PARTITION BY gv.customer_id ORDER BY gv.group_affinity_index DESC) AS number_id,
                         AVG(gv.group_margin) OVER (PARTITION BY gv.customer_id, gv.group_id)            AS group_margin_average
                  FROM groups_view gv)
        SELECT g.customer_id,
               g.group_id,
               g.group_affinity_index,
               g.group_churn_rate,
               g.group_discount_share,
               g.group_minimum_discount,
               g.group_margin_average
        FROM groups g;
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS group_determination(numeric, numeric, numeric);
CREATE OR REPLACE FUNCTION group_determination(max_churn_index numeric, max_transactions_share numeric,
                                               max_margin_share numeric)
    RETURNS TABLE
            (
                Customer_ID          int,
                Group_ID             int,
                Offer_Discount_Depth numeric
            )
AS
$$
DECLARE
    id          int  := 0;
    check_row   bool := FALSE;
    current_row record;
    groups CURSOR FOR (SELECT *
                       FROM get_sorted_groups());
BEGIN
    FOR current_row IN groups
        LOOP
            IF (check_row = TRUE AND id = current_row.customer_id) THEN
                CONTINUE;
            END IF;
            IF (current_row.group_churn_rate <= max_churn_index AND
                current_row.group_discount_share <= max_transactions_share AND
                current_row.group_margin_average * max_margin_share / 100 >=
                CEIL((current_row.group_minimum_discount * 100) / 5.0) * 0.05 * current_row.group_margin_average) THEN
                Customer_ID = current_row.customer_id;
                Group_ID = current_row.group_id;
                Offer_Discount_Depth = CEIL((current_row.group_minimum_discount * 100) / 5.0) * 5;
                check_row = TRUE;
                id = Customer_ID;
                RETURN NEXT;
            ELSE
                check_row = FALSE;
            END IF;
        END LOOP;
END;
$$ LANGUAGE plpgsql;

SELECT *
FROM get_offers(2, '10 / 10 / 2018', '10 / 10 / 2022', 10, 1.15, 3, 70, 30);
