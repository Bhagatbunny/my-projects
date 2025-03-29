CREATE TABLE bun.my_stock_portfolio (
    stock TEXT,
    quantity INT,
    last_price NUMERIC(10,2),
    change NUMERIC(10,2),
    change_percent NUMERIC(10,2),
    buy_price NUMERIC(10,2),
    total_value NUMERIC(15,2),
    total_returns NUMERIC(15,2),
    one_day_returns NUMERIC(15,2),
    pl_percentage NUMERIC(10,2),
	run_id UUID,
	timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);