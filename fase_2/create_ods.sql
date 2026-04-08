CREATE DATABASE IF NOT EXISTS ods_justicia
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE ods_justicia;

-- ========================
-- CATÁLOGOS GEOGRÁFICOS
-- ========================

CREATE TABLE Pais (
    id   INT PRIMARY KEY AUTO_INCREMENT,
    nombre VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS Departamento (
    id      INT PRIMARY KEY AUTO_INCREMENT,
    id_pais INT NOT NULL,
    nombre  VARCHAR(100) NOT NULL,
    FOREIGN KEY (id_pais) REFERENCES Pais(id)
);

CREATE TABLE IF NOT EXISTS Municipio (
    id              INT PRIMARY KEY AUTO_INCREMENT,
    id_departamento INT NOT NULL,
    nombre          VARCHAR(100) NOT NULL,
    FOREIGN KEY (id_departamento) REFERENCES Departamento(id)
);

-- ========================
-- CATÁLOGOS DE PERSONA
-- ========================

CREATE TABLE IF NOT EXISTS Sexo (
    id     INT PRIMARY KEY AUTO_INCREMENT,
    nombre VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS Estado_Civil (
    id     INT PRIMARY KEY AUTO_INCREMENT,
    nombre VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS Grupo_Etnico (
    id     INT PRIMARY KEY AUTO_INCREMENT,
    nombre VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS Relacion (
    id     INT PRIMARY KEY AUTO_INCREMENT,
    nombre VARCHAR(100) NOT NULL
);

-- ========================
-- PERFIL PERSONA
-- ========================

CREATE TABLE IF NOT EXISTS Perfil_Persona (
    id                  INT PRIMARY KEY AUTO_INCREMENT,
    id_municipio_vivienda INT,
    id_sexo             INT,
    id_estado_civil     INT,
    id_grupo_etnico     INT,
    dpi                 VARCHAR(20),
    nit                 VARCHAR(20),
    nombres             VARCHAR(150) NOT NULL,
    apellidos           VARCHAR(150) NOT NULL,
    fecha_nacimiento    DATE,
    direccion           VARCHAR(255),
    telefono            VARCHAR(20),
    FOREIGN KEY (id_municipio_vivienda) REFERENCES Municipio(id),
    FOREIGN KEY (id_sexo)              REFERENCES Sexo(id),
    FOREIGN KEY (id_estado_civil)      REFERENCES Estado_Civil(id),
    FOREIGN KEY (id_grupo_etnico)      REFERENCES Grupo_Etnico(id)
);

-- ========================
-- TRABAJADOR
-- ========================

CREATE TABLE IF NOT EXISTS Trabajador (
    id              INT PRIMARY KEY AUTO_INCREMENT,
    id_perfil_persona INT NOT NULL,
    esta_activo     BOOLEAN NOT NULL DEFAULT TRUE,
    FOREIGN KEY (id_perfil_persona) REFERENCES Perfil_Persona(id)
);

CREATE TABLE IF NOT EXISTS Familia (
    id               INT PRIMARY KEY AUTO_INCREMENT,
    id_trabajador_uno INT NOT NULL,
    id_perfil_persona INT NOT NULL,
    id_relacion       INT NOT NULL,
    FOREIGN KEY (id_trabajador_uno)  REFERENCES Trabajador(id),
    FOREIGN KEY (id_perfil_persona)  REFERENCES Perfil_Persona(id),
    FOREIGN KEY (id_relacion)        REFERENCES Relacion(id)
);

-- ========================
-- INSTITUCIÓN
-- ========================

CREATE TABLE IF NOT EXISTS Tipo_Institucion (
    id     INT PRIMARY KEY AUTO_INCREMENT,
    nombre VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS Nivel_Institucion (
    id     INT PRIMARY KEY AUTO_INCREMENT,
    nombre VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS Institucion (
    id                 INT PRIMARY KEY AUTO_INCREMENT,
    id_municipio       INT,
    id_tipo_institucion INT,
    id_nivel_institucion INT,
    nombre             VARCHAR(200) NOT NULL,
    siglas             VARCHAR(30),
    direccion          VARCHAR(255),
    telefono           VARCHAR(20),
    fecha_creacion     DATE,
    FOREIGN KEY (id_municipio)        REFERENCES Municipio(id),
    FOREIGN KEY (id_tipo_institucion) REFERENCES Tipo_Institucion(id),
    FOREIGN KEY (id_nivel_institucion) REFERENCES Nivel_Institucion(id)
);

-- ========================
-- CARGO Y CONTRATO
-- ========================

CREATE TABLE IF NOT EXISTS Cargo (
    id                   INT PRIMARY KEY AUTO_INCREMENT,
    id_institucion        INT NOT NULL,
    id_nivel_cargo        INT,
    id_cargo_jefe         INT,
    nombre               VARCHAR(150) NOT NULL,
    descripcion          TEXT,
    sueldo_base          DECIMAL(12,2),
    renglon_presupuestario VARCHAR(20),
    fecha_creacion       DATE,
    FOREIGN KEY (id_institucion) REFERENCES Institucion(id),
    FOREIGN KEY (id_cargo_jefe)  REFERENCES Cargo(id)
);

CREATE TABLE IF NOT EXISTS Tipo_Contrato (
    id     INT PRIMARY KEY AUTO_INCREMENT,
    nombre VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS Estado_Contrato (
    id     INT PRIMARY KEY AUTO_INCREMENT,
    nombre VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS Contrato (
    id               INT PRIMARY KEY AUTO_INCREMENT,
    id_trabajador    INT NOT NULL,
    id_empresa       INT,
    id_institucion   INT,
    id_cargo         INT,
    id_tipo_contrato INT,
    id_estado_contrato INT,
    sueldo_real      DECIMAL(12,2),
    fecha_inicio     DATE,
    fecha_fin        DATE,
    FOREIGN KEY (id_trabajador)     REFERENCES Trabajador(id),
    FOREIGN KEY (id_institucion)    REFERENCES Institucion(id),
    FOREIGN KEY (id_cargo)          REFERENCES Cargo(id),
    FOREIGN KEY (id_tipo_contrato)  REFERENCES Tipo_Contrato(id),
    FOREIGN KEY (id_estado_contrato) REFERENCES Estado_Contrato(id)
    -- id_empresa se agrega FK después de crear Empresa
);

-- ========================
-- EMPRESA
-- ========================

CREATE TABLE IF NOT EXISTS Tipo_Empresa (
    id     INT PRIMARY KEY AUTO_INCREMENT,
    nombre VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS Empresa (
    id                  INT PRIMARY KEY AUTO_INCREMENT,
    id_perfil_persona_repr INT,
    id_tipo_empresa     INT,
    ingresos            DECIMAL(15,2),
    egresos             DECIMAL(15,2),
    nit                 VARCHAR(20),
    direccion           VARCHAR(255),
    telefono            VARCHAR(20),
    estado              VARCHAR(50),
    observaciones       TEXT,
    fecha_registro      DATE,
    FOREIGN KEY (id_perfil_persona_repr) REFERENCES Perfil_Persona(id),
    FOREIGN KEY (id_tipo_empresa)        REFERENCES Tipo_Empresa(id)
);

-- FK diferida de Contrato -> Empresa
ALTER TABLE Contrato
    ADD CONSTRAINT fk_contrato_empresa
    FOREIGN KEY (id_empresa) REFERENCES Empresa(id);

-- ========================
-- DECLARACIÓN PATRIMONIAL
-- ========================

CREATE TABLE IF NOT EXISTS Tipo_Declaracion (
    id     INT PRIMARY KEY AUTO_INCREMENT,
    nombre VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS Declaracion_Patrimonial (
    id                 INT PRIMARY KEY AUTO_INCREMENT,
    id_trabajador      INT NOT NULL,
    id_tipo_declaracion INT NOT NULL,
    ingresos           DECIMAL(15,2),
    egresos            DECIMAL(15,2),
    activos            DECIMAL(15,2),
    pasivos            DECIMAL(15,2),
    vehiculos          INT,
    observaciones      TEXT,
    fecha_presentacion DATE,
    FOREIGN KEY (id_trabajador)       REFERENCES Trabajador(id),
    FOREIGN KEY (id_tipo_declaracion) REFERENCES Tipo_Declaracion(id)
);

-- ========================
-- PAGOS
-- ========================

CREATE TABLE IF NOT EXISTS Tipo_Pago (
    id     INT PRIMARY KEY AUTO_INCREMENT,
    nombre VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS Pagos_Realizados (
    id             INT PRIMARY KEY AUTO_INCREMENT,
    id_trabajador  INT NOT NULL,
    id_tipo_pago   INT NOT NULL,
    pago_realizado DECIMAL(12,2),
    fecha_pago     DATE,
    FOREIGN KEY (id_trabajador) REFERENCES Trabajador(id),
    FOREIGN KEY (id_tipo_pago)  REFERENCES Tipo_Pago(id)
);

-- ========================
-- INDICADOR EXTERNO
-- ========================

CREATE TABLE IF NOT EXISTS Indicador_Externo (
    id             INT PRIMARY KEY AUTO_INCREMENT,
    anio           YEAR NOT NULL,
    fuente         VARCHAR(150),
    tipo_indicador VARCHAR(100),
    valor          DECIMAL(15,4),
    unidad         VARCHAR(50)
);

-- ========================
-- DENUNCIA
-- ========================

CREATE TABLE IF NOT EXISTS Tipo_Denuncia (
    id     INT PRIMARY KEY AUTO_INCREMENT,
    nombre VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS Estado_Denuncia (
    id     INT PRIMARY KEY AUTO_INCREMENT,
    nombre VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS Tipo_Implicado (
    id          INT PRIMARY KEY AUTO_INCREMENT,
    nombre      VARCHAR(100) NOT NULL,
    descripcion TEXT
);

CREATE TABLE IF NOT EXISTS Denuncia (
    id               INT PRIMARY KEY AUTO_INCREMENT,
    id_tipo_denuncia INT,
    id_estado_denuncia INT,
    id_institucion   INT,
    descripcion      TEXT,
    FOREIGN KEY (id_tipo_denuncia)   REFERENCES Tipo_Denuncia(id),
    FOREIGN KEY (id_estado_denuncia) REFERENCES Estado_Denuncia(id),
    FOREIGN KEY (id_institucion)     REFERENCES Institucion(id)
);

CREATE TABLE IF NOT EXISTS Implicado (
    id               INT PRIMARY KEY AUTO_INCREMENT,
    id_denuncia      INT NOT NULL,
    id_perfil_persona INT NOT NULL,
    id_tipo_implicado INT,
    FOREIGN KEY (id_denuncia)       REFERENCES Denuncia(id),
    FOREIGN KEY (id_perfil_persona) REFERENCES Perfil_Persona(id),
    FOREIGN KEY (id_tipo_implicado) REFERENCES Tipo_Implicado(id)
);

-- ========================
-- REGISTRO DE EVENTOS / FECHAS
-- ========================

CREATE TABLE IF NOT EXISTS Tipo_Evento (
    id          INT PRIMARY KEY AUTO_INCREMENT,
    nombre      VARCHAR(100) NOT NULL,
    descripcion TEXT
);

CREATE TABLE IF NOT EXISTS Registro_Fecha (
    id          INT PRIMARY KEY AUTO_INCREMENT,
    id_denuncia INT NOT NULL,
    id_tipo_evento INT NOT NULL,
    fecha       DATE NOT NULL,
    FOREIGN KEY (id_denuncia)    REFERENCES Denuncia(id),
    FOREIGN KEY (id_tipo_evento) REFERENCES Tipo_Evento(id)
);

-- ========================
-- PROCESO JUDICIAL
-- ========================

CREATE TABLE IF NOT EXISTS Estado_Proceso (
    id     INT PRIMARY KEY AUTO_INCREMENT,
    nombre VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS Proceso_Judicial (
    id               INT PRIMARY KEY AUTO_INCREMENT,
    id_denuncia      INT NOT NULL,
    id_institucion   INT,
    id_estado_proceso INT,
    FOREIGN KEY (id_denuncia)       REFERENCES Denuncia(id),
    FOREIGN KEY (id_institucion)    REFERENCES Institucion(id),
    FOREIGN KEY (id_estado_proceso) REFERENCES Estado_Proceso(id)
);

CREATE TABLE IF NOT EXISTS Rol_Proceso (
    id     INT PRIMARY KEY AUTO_INCREMENT,
    nombre VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS Trabajador_Implicado (
    id               INT PRIMARY KEY AUTO_INCREMENT,
    id_proceso_judicial INT NOT NULL,
    id_rol_proceso   INT,
    id_trabajador    INT NOT NULL,
    FOREIGN KEY (id_proceso_judicial) REFERENCES Proceso_Judicial(id),
    FOREIGN KEY (id_rol_proceso)      REFERENCES Rol_Proceso(id),
    FOREIGN KEY (id_trabajador)       REFERENCES Trabajador(id)
);

-- ========================
-- SENTENCIA
-- ========================

CREATE TABLE IF NOT EXISTS Tipo_Sentencia (
    id     INT PRIMARY KEY AUTO_INCREMENT,
    nombre VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS Estado_Sentencia (
    id     INT PRIMARY KEY AUTO_INCREMENT,
    nombre VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS Sentencia (
    id                 INT PRIMARY KEY AUTO_INCREMENT,
    id_proceso_judicial INT NOT NULL,
    id_tipo_sentencia  INT,
    id_estado_sentencia INT,
    descripcion        TEXT,
    fecha_creacion     DATE,
    FOREIGN KEY (id_proceso_judicial) REFERENCES Proceso_Judicial(id),
    FOREIGN KEY (id_tipo_sentencia)   REFERENCES Tipo_Sentencia(id),
    FOREIGN KEY (id_estado_sentencia) REFERENCES Estado_Sentencia(id)
);

-- ========================
-- AUDIENCIA
-- ========================

CREATE TABLE IF NOT EXISTS Tipo_Audiencia (
    id     INT PRIMARY KEY AUTO_INCREMENT,
    nombre VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS Estado_Audiencia (
    id     INT PRIMARY KEY AUTO_INCREMENT,
    nombre VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS Audiencia (
    id                  INT PRIMARY KEY AUTO_INCREMENT,
    id_proceso_judicial INT NOT NULL,
    id_tipo_audiencia   INT,
    id_estado_audiencia INT,
    descripcion         TEXT,
    observaciones       TEXT,
    fecha_creacion      DATE,
    FOREIGN KEY (id_proceso_judicial) REFERENCES Proceso_Judicial(id),
    FOREIGN KEY (id_tipo_audiencia)   REFERENCES Tipo_Audiencia(id),
    FOREIGN KEY (id_estado_audiencia) REFERENCES Estado_Audiencia(id)
);

-- ========================
-- DOCUMENTO
-- ========================

CREATE TABLE IF NOT EXISTS Tipo_Documento (
    id     INT PRIMARY KEY AUTO_INCREMENT,
    nombre VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS Documento (
    id                  INT PRIMARY KEY AUTO_INCREMENT,
    id_proceso_judicial INT NOT NULL,
    id_tipo_documento   INT,
    titulo              VARCHAR(255),
    descripcion         TEXT,
    fecha_presentacion  DATE,
    FOREIGN KEY (id_proceso_judicial) REFERENCES Proceso_Judicial(id),
    FOREIGN KEY (id_tipo_documento)   REFERENCES Tipo_Documento(id)
);
