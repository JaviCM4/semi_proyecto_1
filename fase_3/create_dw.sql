-- ========================
-- DIMENSIONES COMPARTIDAS
-- ========================

CREATE TABLE Dim_Tiempo (
    sk_tiempo       INT PRIMARY KEY,
    fecha           DATE NOT NULL,
    anio            SMALLINT NOT NULL,
    trimestre       TINYINT NOT NULL,
    mes             TINYINT NOT NULL,
    nombre_mes      VARCHAR(20) NOT NULL,
    semana          TINYINT NOT NULL,
    es_fin_semana   BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE Dim_Institucion (
    sk_institucion      INT PRIMARY KEY,
    id_institucion_ods  INT NOT NULL,
    nombre              VARCHAR(200) NOT NULL,
    siglas              VARCHAR(30),
    tipo_institucion    VARCHAR(100),
    nivel_institucion   VARCHAR(100),
    departamento        VARCHAR(100),
    municipio           VARCHAR(100)
);

CREATE TABLE Dim_Trabajador (
    sk_trabajador       INT PRIMARY KEY,
    id_trabajador_ods   INT NOT NULL,
    nombres_completos   VARCHAR(300) NOT NULL,
    cargo_nombre        VARCHAR(150),
    nivel_cargo         VARCHAR(100),
    rol_proceso         VARCHAR(100),
    institucion_nombre  VARCHAR(200),
    tipo_institucion    VARCHAR(100),
    departamento        VARCHAR(100),
    esta_activo         BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE Dim_Cargo (
    sk_cargo                INT PRIMARY KEY,
    id_cargo_ods            INT NOT NULL,
    nombre                  VARCHAR(150) NOT NULL,
    descripcion             TEXT,
    sueldo_base             DECIMAL(12,2),
    renglon_presupuestario  VARCHAR(20),
    nivel_cargo             VARCHAR(100)
);

-- ============================
-- DIMENSIONES DE Hecho_Proceso
-- ============================

CREATE TABLE Dim_Tipo_Delito (
    sk_tipo_delito  INT PRIMARY KEY,
    nombre_delito   VARCHAR(150) NOT NULL,
    categoria       VARCHAR(100),
    es_corrupto     BOOLEAN NOT NULL DEFAULT FALSE,
    es_alto_impacto BOOLEAN NOT NULL DEFAULT FALSE,
    descripcion     TEXT
);

CREATE TABLE Dim_Tipo_Implicado (
    sk_tipo_implicado   INT PRIMARY KEY,
    nombre              VARCHAR(100) NOT NULL,
    descripcion         TEXT
);

CREATE TABLE Dim_Estado_Proceso (
    sk_tipo_implicado   INT PRIMARY KEY,
    nombre              VARCHAR(100) NOT NULL
);

-- ===============================
-- DIMENSIONES DE Hecho_Patrimonio
-- ===============================

CREATE TABLE Dim_Tipo_Declaracion (
    sk_trabajo  INT PRIMARY KEY,
    nombre      VARCHAR(100) NOT NULL
);

-- =============================
-- DIMENSIONES DE Hecho_Contrato
-- =============================

CREATE TABLE Dim_Empresa (
    sk_empresa              INT PRIMARY KEY,
    id_empresa_ods          INT NOT NULL,
    nombre                  VARCHAR(200) NOT NULL,
    nit                     VARCHAR(20),
    tipo_empresa            VARCHAR(100),
    nombre_representante    VARCHAR(300),
    fecha_constitucion      DATE,
    departamento            VARCHAR(100)
);

-- Dim_Relacion se usa en Hecho_Contrato y Hecho_Nepotismo
CREATE TABLE Dim_Relacion (
    sk_relacion INT PRIMARY KEY,
    nombre      VARCHAR(100) NOT NULL
);

CREATE TABLE Dim_Tipo_Contrato (
    sk_tipo_contrato    INT PRIMARY KEY,
    nombre              VARCHAR(100) NOT NULL
);

-- ==============================
-- DIMENSIONES DE Hecho_Nepotismo
-- ==============================

CREATE TABLE Dim_Trabajador_Familiar (
    sk_relacion         INT PRIMARY KEY,
    id_trabajador_ods   INT NOT NULL,
    nombres_completos   VARCHAR(300) NOT NULL,
    cargo_nombre        VARCHAR(150),
    institucion_nombre  VARCHAR(200),
    departamento        VARCHAR(100)
);

-- ========================
-- TABLAS DE HECHOS
-- ========================

-- Hecho_Proceso: analiza procesos judiciales
CREATE TABLE Hecho_Proceso (
    sk_proceso          INT PRIMARY KEY,
    sk_tiempo           INT NOT NULL,
    sk_trabajador       INT NOT NULL,
    sk_institucion      INT NOT NULL,
    sk_tipo_delito      INT NOT NULL,
    sk_tipo_implicado   INT NOT NULL,
    sk_estado_proceso   INT NOT NULL,
    -- métricas
    dias_proceso        INT,
    cantidad_audiencias INT,
    fue_condenado       BOOLEAN NOT NULL DEFAULT FALSE,
    fue_sobreseido      BOOLEAN NOT NULL DEFAULT FALSE,
    fue_archivado       BOOLEAN NOT NULL DEFAULT FALSE,
    fue_apelado         BOOLEAN NOT NULL DEFAULT FALSE,
    FOREIGN KEY (sk_tiempo)         REFERENCES Dim_Tiempo(sk_tiempo),
    FOREIGN KEY (sk_trabajador)     REFERENCES Dim_Trabajador(sk_trabajador),
    FOREIGN KEY (sk_institucion)    REFERENCES Dim_Institucion(sk_institucion),
    FOREIGN KEY (sk_tipo_delito)    REFERENCES Dim_Tipo_Delito(sk_tipo_delito),
    FOREIGN KEY (sk_tipo_implicado) REFERENCES Dim_Tipo_Implicado(sk_tipo_implicado),
    FOREIGN KEY (sk_estado_proceso) REFERENCES Dim_Estado_Proceso(sk_tipo_implicado)
);

-- Hecho_Patrimonio: analiza declaraciones patrimoniales vs. sueldos
CREATE TABLE Hecho_Patrimonio (
    sk_patrimonio           INT PRIMARY KEY,
    sk_tiempo               INT NOT NULL,
    sk_trabajador           INT NOT NULL,
    sk_institucion          INT NOT NULL,
    sk_cargo                INT NOT NULL,
    sk_tipo_declaracion     INT NOT NULL,
    -- métricas
    sueldo                  DECIMAL(12,2),
    pago_realizado          DECIMAL(12,2),
    ingresos_declarados     DECIMAL(15,2),
    egresos_declarados      DECIMAL(15,2),
    activos_declarados      DECIMAL(15,2),
    vehiculos_declarados    INT,
    patrimonio_neto         DECIMAL(15,2),
    enriquecimiento         DECIMAL(15,2),
    FOREIGN KEY (sk_tiempo)             REFERENCES Dim_Tiempo(sk_tiempo),
    FOREIGN KEY (sk_trabajador)         REFERENCES Dim_Trabajador(sk_trabajador),
    FOREIGN KEY (sk_institucion)        REFERENCES Dim_Institucion(sk_institucion),
    FOREIGN KEY (sk_cargo)              REFERENCES Dim_Cargo(sk_cargo),
    FOREIGN KEY (sk_tipo_declaracion)   REFERENCES Dim_Tipo_Declaracion(sk_trabajo)
);

-- Hecho_Contrato: analiza contratos entre trabajadores y empresas
CREATE TABLE Hecho_Contrato (
    sk_contrato             INT PRIMARY KEY,
    sk_tiempo               INT NOT NULL,
    sk_trabajador           INT NOT NULL,
    sk_institucion          INT NOT NULL,
    sk_cargo                INT NOT NULL,
    sk_empresa              INT NOT NULL,
    sk_tipo_contrato        INT NOT NULL,
    sk_relacion             INT,
    -- métricas
    monto_contrato          DECIMAL(15,2),
    duracion_dias           INT,
    es_empresa_vinculada    BOOLEAN NOT NULL DEFAULT FALSE,
    grado_vinculacion       INT,
    cantidad_contratos_emp  INT,
    monto_acumulado_emp     DECIMAL(15,2),
    FOREIGN KEY (sk_tiempo)         REFERENCES Dim_Tiempo(sk_tiempo),
    FOREIGN KEY (sk_trabajador)     REFERENCES Dim_Trabajador(sk_trabajador),
    FOREIGN KEY (sk_institucion)    REFERENCES Dim_Institucion(sk_institucion),
    FOREIGN KEY (sk_cargo)          REFERENCES Dim_Cargo(sk_cargo),
    FOREIGN KEY (sk_empresa)        REFERENCES Dim_Empresa(sk_empresa),
    FOREIGN KEY (sk_tipo_contrato)  REFERENCES Dim_Tipo_Contrato(sk_tipo_contrato),
    FOREIGN KEY (sk_relacion)       REFERENCES Dim_Relacion(sk_relacion)
);

-- Hecho_Nepotismo: detecta vínculos familiares dentro del sistema
CREATE TABLE Hecho_Nepotismo (
    sk_nepotismo            INT PRIMARY KEY,
    sk_tiempo               INT NOT NULL,
    sk_trabajador           INT NOT NULL,
    sk_institucion          INT NOT NULL,
    sk_cargo                INT NOT NULL,
    sk_trabajador_familiar  INT NOT NULL,
    sk_relacion             INT NOT NULL,
    -- métricas
    dias_diferencia_ingreso INT,
    misma_institucion       BOOLEAN NOT NULL DEFAULT FALSE,
    mismo_departamento      BOOLEAN NOT NULL DEFAULT FALSE,
    sueldo_titular          DECIMAL(12,2),
    sueldo_familiar         DECIMAL(12,2),
    cantidad_familiares_sis INT,
    FOREIGN KEY (sk_tiempo)             REFERENCES Dim_Tiempo(sk_tiempo),
    FOREIGN KEY (sk_trabajador)         REFERENCES Dim_Trabajador(sk_trabajador),
    FOREIGN KEY (sk_institucion)        REFERENCES Dim_Institucion(sk_institucion),
    FOREIGN KEY (sk_cargo)              REFERENCES Dim_Cargo(sk_cargo),
    FOREIGN KEY (sk_trabajador_familiar) REFERENCES Dim_Trabajador_Familiar(sk_relacion),
    FOREIGN KEY (sk_relacion)           REFERENCES Dim_Relacion(sk_relacion)
);