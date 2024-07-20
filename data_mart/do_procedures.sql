
-- 4)
DO $$
DECLARE
    d DATE := '2018-01-01';
BEGIN
    WHILE d <= '2018-01-31' LOOP
        CALL "DM".fill_account_balance_f(d);
        d := d + INTERVAL '1 day';
    END LOOP;
END $$;


-- 5)
DO $$
DECLARE
d DATE := '2018-01-01';
BEGIN
    WHILE d <= '2018-01-31' LOOP
        CALL "DM".fill_account_turnover_f(d);
        d := d + INTERVAL '1 day';
    END LOOP;
END $$;


-- 6)
DO $$
DECLARE
    d DATE := '2018-02-01';
BEGIN
    CALL "DM".fill_f101_round_f(d);
END $$;
