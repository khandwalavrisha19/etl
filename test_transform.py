import sys
sys.path.append('c:/Users/ruchi/ETL2')
from transform import RoutineTransformer

proc = """
CREATE DEFINER=`root`@`localhost` PROCEDURE `sp_place_order`(p_customer_id INT, p_order_date TIMESTAMP, p_total DECIMAL(10,2))
BEGIN
    DECLARE v_exists INT DEFAULT 0;
    SELECT COUNT(*) INTO v_exists FROM customers WHERE customer_id = p_customer_id;
    IF v_exists > 0 THEN
        INSERT INTO orders (customer_id, order_date, total) VALUES (p_customer_id, p_order_date, p_total);
    END IF;
END
"""

func = """
CREATE DEFINER=`root`@`localhost` FUNCTION `fn_customer_active_orders_count`(p_customer_id INT) RETURNS int(11)
    DETERMINISTIC
BEGIN
    DECLARE v_count INT;
    SELECT COUNT(*) INTO v_count FROM orders WHERE customer_id = p_customer_id AND status IN ('pending', 'processing', 'shipped');
    RETURN v_count;
END
"""

t = RoutineTransformer()
print("============== PROCEDURE ==============")
print(t.transform_procedure('sp_place_order', proc))
print("\n============== FUNCTION ==============")
print(t.transform_function('fn_customer_active_orders_count', func))
