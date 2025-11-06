-- Script de inicialización para TFM
CREATE TABLE IF NOT EXISTS usuarios_tfm (
    id SERIAL PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL,
    email VARCHAR(150) UNIQUE NOT NULL,
    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS proyectos_tfm (
    id SERIAL PRIMARY KEY,
    nombre_proyecto VARCHAR(200) NOT NULL,
    descripcion TEXT,
    usuario_id INTEGER REFERENCES usuarios_tfm(id),
    fecha_inicio DATE,
    fecha_fin DATE,
    estado VARCHAR(50) DEFAULT 'activo'
);

-- Datos de ejemplo para TFM
INSERT INTO usuarios_tfm (nombre, email) VALUES 
('Investigador Principal', 'investigador@tfm.com'),
('Asistente Investigación', 'asistente@tfm.com')
ON CONFLICT (email) DO NOTHING;