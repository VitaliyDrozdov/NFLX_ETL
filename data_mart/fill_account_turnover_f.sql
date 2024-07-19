
DROP TABLE IF EXISTS "DM".account_turnover_f;

CREATE TABLE IF NOT EXISTS "DM".account_turnover_f (
    on_date DATE NOT NULL,
    account_rk NUMERIC NOT NULL,
    credit_amount NUMERIC(23, 8),
    credit_amount_rub NUMERIC(23, 8),
    debet_amount NUMERIC(23, 8),
    debet_amount_rub NUMERIC(23, 8)
);



CREATE OR REPLACE PROCEDURE fill_account_turnover_f (i_OnDate DATE)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO "DM".account_turnover_f (on_date, account_rk, credit_amount, credit_amount_rub, debet_amount, debet_amount_rub)
    SELECT i_OnDate,
            account_rk,
            SUM(credit_amount) as credit_amount,
            SUM(credit_amount) * (COALESCE(MAX(ex.reduced_cource), 1)) as credit_amount_rub,
            SUM(debet_amount) AS debet_amount,
            SUM(debet_amount)  * (COALESCE(MAX(ex.reduced_cource), 1)) as debet_amount_rub
    FROM  (
        SELECT
            p.credit_account_rk as account_rk,
            p.credit_amount,
            0 as debet_amount,
            a.currency_rk
        FROM "DS".ft_posting_f p
        JOIN "DS".md_account_d a on p.credit_account_rk = a.account_rk
        WHERE i_OnDate = p.oper_date

        UNION ALL

        SELECT
            p.debet_account_rk as account_rk,
            0 as credit_amount,
            p.debet_amount,
            a.currency_rk
        FROM "DS".ft_posting_f p
        JOIN "DS".md_account_d a on p.debet_account_rk = a.account_rk
        WHERE i_OnDate = p.oper_date
    ) as turnover
    LEFT JOIN "DS".md_exchange_rate_d ex on ex.currency_rk = turnover.currency_rk
    AND i_OnDate >= ex.data_actual_date
    AND (ex.data_actual_end_date IS NULL OR i_OnDate <= ex.data_actual_end_date)
    GROUP BY account_rk;
END;
$$;

DROP TABLE  IF EXISTS "DM".account_balance_f;

CREATE TABLE  IF NOT EXISTS "DM".account_balance_f (
    on_date DATE NOT NULL,
    account_rk NUMERIC NOT NULL,
    balance_out NUMERIC,
    balance_out_rub FLOAT
);



INSERT INTO "DM".account_balance_f (on_date, account_rk, balance_out, balance_out_rub)
SELECT
    '2017-12-31',
    account_rk,
    balance_out,
    balance_out * COALESCE(reduced_cource, 1) AS balance_out_rub
FROM
    "DS".ft_balance_f bal
LEFT JOIN
    "DS".md_exchange_rate_d ex
    ON
    bal.currency_rk = ex.currency_rk
    AND '2017-12-31' >= ex.data_actual_date
    AND ('2017-12-31' <= ex.data_actual_end_date OR ex.data_actual_end_date IS NULL);



DO $$
DECLARE
d DATE := '2018-01-01';
BEGIN
    WHILE d <= '2018-01-31' LOOP
        CALL "DS".fill_account_turnover_f(d);
        d := d + INTERVAL '1 day';
    END LOOP;
END $$;
