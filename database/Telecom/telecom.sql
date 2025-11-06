-- =========================================
-- CREACIÓN DE TABLAS PARA ANÁLISIS DE CHURN
-- =========================================
 -- Tabla principal de clientes

CREATE TABLE customers ( customer_id SERIAL PRIMARY KEY,
                                                    age INTEGER, gender VARCHAR(10),
                                                                        location VARCHAR(50),
                                                                                 created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);

-- Tabla de suscripciones

CREATE TABLE subscriptions ( subscription_id SERIAL PRIMARY KEY,
                                                            customer_id INTEGER REFERENCES customers(customer_id),
                                                                                           plan_type VARCHAR(50),
                                                                                                     tenure_months INTEGER, monthly_charges DECIMAL(10,2),
                                                                                                                                            billing_cycle VARCHAR(20),
                                                                                                                                                          start_date DATE);

-- Tabla de uso

CREATE TABLE usage_data ( usage_id SERIAL PRIMARY KEY,
                                                  customer_id INTEGER REFERENCES customers(customer_id),
                                                                                 total_call_minutes INTEGER, data_usage_gb DECIMAL(10,2),
                                                                                                                           num_sms_sent INTEGER, streaming_services BOOLEAN, roaming_usage DECIMAL(10,2),
                                                                                                                                                                                           usage_month DATE DEFAULT CURRENT_DATE);

-- Tabla de soporte al cliente

CREATE TABLE customer_support ( support_id SERIAL PRIMARY KEY,
                                                          customer_id INTEGER REFERENCES customers(customer_id),
                                                                                         customer_service_calls INTEGER, customer_feedback_score INTEGER, network_quality_score DECIMAL(3,1),
                                                                                                                                                                                interaction_date DATE DEFAULT CURRENT_DATE);

-- Tabla de pagos

CREATE TABLE payments ( payment_id SERIAL PRIMARY KEY,
                                                  customer_id INTEGER REFERENCES customers(customer_id),
                                                                                 payment_method VARCHAR(30),
                                                                                                payment_failures INTEGER, discount_applied BOOLEAN, payment_date DATE DEFAULT CURRENT_DATE);

-- Tabla de características adicionales

CREATE TABLE additional_features ( feature_id SERIAL PRIMARY KEY,
                                                             customer_id INTEGER REFERENCES customers(customer_id),
                                                                                            device_type VARCHAR(30),
                                                                                                        multiple_lines BOOLEAN, family_plan BOOLEAN, premium_support BOOLEAN, churn BOOLEAN);

-- =========================================
-- VISTA DE ANÁLISIS DE CHURN
-- =========================================

CREATE VIEW churn_analysis AS
SELECT c.customer_id,
       c.age,
       c.gender,
       c.location,
       s.plan_type,
       s.tenure_months,
       s.monthly_charges,
       u.total_call_minutes,
       u.data_usage_gb,
       cs.customer_service_calls,
       cs.customer_feedback_score,
       p.payment_failures,
       af.device_type,
       af.multiple_lines,
       af.family_plan,
       af.premium_support,
       af.churn
FROM customers c
LEFT JOIN subscriptions s ON c.customer_id = s.customer_id
LEFT JOIN usage_data u ON c.customer_id = u.customer_id
LEFT JOIN customer_support cs ON c.customer_id = cs.customer_id
LEFT JOIN payments p ON c.customer_id = p.customer_id
LEFT JOIN additional_features af ON c.customer_id = af.customer_id;

