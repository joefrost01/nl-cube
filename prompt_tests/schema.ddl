CREATE TABLE orders (
                        order_id UUID,
                        customer_id UUID,
                        order_date DATE,
                        product_category STRING,
                        product_id STRING,
                        region STRING,
                        quantity INTEGER,
                        unit_price DOUBLE,
                        discount DOUBLE,
                        delivery_date DATE,
                        courier_name STRING
);

CREATE TABLE returns (
                         return_id UUID,
                         order_id UUID,
                         return_date DATE,
                         return_reason STRING,
                         refund_amount DOUBLE
);
