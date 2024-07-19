CREATE OR REPLACE PROCEDURE "DM".fill_account_balance_f (i_OnDate DATE)
LANGUAGE plpgsql
AS $$
BEGIN

    DELETE FROM "DM".account_balance_f WHERE on_date = i_OnDate;

    INSERT INTO "DM".account_balance_f (on_date, account_rk, balance_out, balance_out_rub)
    SELECT
        i_OnDate AS on_date,
        a.account_rk,
        CASE
            WHEN a.char_type = 'А' THEN COALESCE(prev_balance.balance_out, 0) + COALESCE(t.debet_amount, 0) - COALESCE(t.credit_amount, 0)
            WHEN a.char_type = 'П' THEN COALESCE(prev_balance.balance_out, 0) - COALESCE(t.debet_amount, 0) + COALESCE(t.credit_amount, 0)
            ELSE 0
        END AS balance_out,
        CASE
            WHEN a.char_type = 'А' THEN (COALESCE(prev_balance.balance_out_rub, 0) + COALESCE(t.debet_amount_rub, 0) - COALESCE(t.credit_amount_rub, 0))
            WHEN a.char_type = 'П' THEN (COALESCE(prev_balance.balance_out_rub, 0) - COALESCE(t.debet_amount_rub, 0) + COALESCE(t.credit_amount_rub, 0))
            ELSE 0
        END AS balance_out_rub
    FROM
        "DS".md_account_d a
    LEFT JOIN LATERAL (
        SELECT
            account_rk,
            balance_out,
            balance_out_rub
        FROM
            "DM".account_balance_f
        WHERE
            account_rk = a.account_rk
            AND on_date = i_OnDate - INTERVAL '1 day'
        ORDER BY
            on_date DESC
        LIMIT 1
    ) AS prev_balance ON true
    LEFT JOIN LATERAL (
        SELECT
            account_rk,
            SUM(COALESCE(credit_amount, 0)) AS credit_amount,
            SUM(COALESCE(credit_amount_rub, 0)) AS credit_amount_rub,
            SUM(COALESCE(debet_amount, 0)) AS debet_amount,
            SUM(COALESCE(debet_amount_rub, 0)) AS debet_amount_rub
        FROM
            "DM".account_turnover_f
        WHERE
            on_date = i_OnDate
        GROUP BY
            account_rk
    ) AS t ON t.account_rk = a.account_rk
    WHERE
        i_OnDate >= a.data_actual_date
        AND (a.data_actual_end_date IS NULL OR i_OnDate <= a.data_actual_end_date);
END;
$$;



DO $$
DECLARE
    d DATE := '2018-01-01';
BEGIN
    WHILE d <= '2018-01-31' LOOP
        CALL "DM".fill_account_balance_f(d);
        d := d + INTERVAL '1 day';
    END LOOP;
END $$;
