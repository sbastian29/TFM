-- =========================================
-- CREACIÓN DE TABLAS PARA ANÁLISIS DE CHURN 
-- =========================================

-- IMPORTANTE: customer_id NO es SERIAL porque viene de los datos de Kafka

-- Tabla principal de clientes
CREATE TABLE customers (
    customer_id INTEGER PRIMARY KEY,  
    gender VARCHAR(10),
    location VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabla de suscripciones
CREATE TABLE subscriptions (
    subscription_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(customer_id) ON DELETE CASCADE,
    plan_type VARCHAR(50),
    tenure_months INTEGER,
    monthly_charges DECIMAL(10,2),
    billing_cycle VARCHAR(20),
    start_date DATE
);

-- Tabla de uso
CREATE TABLE usage_data (
    usage_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(customer_id) ON DELETE CASCADE,
    total_call_minutes INTEGER,
    data_usage_gb DECIMAL(10,2),
    num_sms_sent INTEGER,
    streaming_services BOOLEAN,
    roaming_usage DECIMAL(10,2),
    usage_month DATE DEFAULT CURRENT_DATE
);

-- Tabla de soporte al cliente
CREATE TABLE customer_support (
    support_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(customer_id) ON DELETE CASCADE,
    customer_service_calls INTEGER,
    customer_feedback_score INTEGER,
    network_quality_score DECIMAL(3,1),
    interaction_date DATE DEFAULT CURRENT_DATE
);

-- Tabla de pagos
CREATE TABLE payments (
    payment_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(customer_id) ON DELETE CASCADE,
    payment_method VARCHAR(30),
    payment_failures INTEGER,
    discount_applied BOOLEAN,
    payment_date DATE DEFAULT CURRENT_DATE
);

-- Tabla de características adicionales
CREATE TABLE additional_features (
    feature_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(customer_id) ON DELETE CASCADE,
    device_type VARCHAR(30),
    multiple_lines BOOLEAN,
    family_plan BOOLEAN,
    premium_support BOOLEAN,
    churn BOOLEAN
);

-- =========================================
-- VISTA DE ANÁLISIS DE CHURN
-- =========================================
CREATE VIEW churn_analysis AS
SELECT 
    c.customer_id,
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

-- =========================================
-- ÍNDICES PARA MEJORAR RENDIMIENTO
-- =========================================
CREATE INDEX idx_subscriptions_customer ON subscriptions(customer_id);
CREATE INDEX idx_usage_customer ON usage_data(customer_id);
CREATE INDEX idx_support_customer ON customer_support(customer_id);
CREATE INDEX idx_payments_customer ON payments(customer_id);
CREATE INDEX idx_features_customer ON additional_features(customer_id);
CREATE INDEX idx_churn ON additional_features(churn);


-- Tabla para registrar errores de calidad de datos
CREATE TABLE IF NOT EXISTS data_quality_errors (
    id SERIAL PRIMARY KEY,
    batch_id INTEGER,
    error_reason VARCHAR(255),
    error_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    raw_data TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_error_batch ON data_quality_errors(batch_id);
CREATE INDEX idx_error_reason ON data_quality_errors(error_reason);
CREATE INDEX idx_error_timestamp ON data_quality_errors(error_timestamp);
