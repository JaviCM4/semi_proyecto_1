-- ============================================================
-- 00_catalogos_base.sql
-- Tablas de catálogo sin dependencias externas
-- Ejecutar PRIMERO, antes de 00b_geografia.sql y los demás
-- ============================================================

USE ods_justicia;
SET FOREIGN_KEY_CHECKS = 0;

-- ============================================================
-- País (base de la jerarquía geográfica)
-- ============================================================
INSERT INTO Pais (id, nombre) VALUES
  (1, 'Guatemala');

-- ============================================================
-- Sexo
-- ============================================================
INSERT INTO Sexo (id, nombre) VALUES
  (1, 'Masculino'),
  (2, 'Femenino'),
  (3, 'No especificado');

-- ============================================================
-- Estado_Civil
-- ============================================================
INSERT INTO Estado_Civil (id, nombre) VALUES
  (1, 'Soltero/a'),
  (2, 'Casado/a'),
  (3, 'Divorciado/a'),
  (4, 'Viudo/a'),
  (5, 'Unión libre');

-- ============================================================
-- Grupo_Etnico
-- ============================================================
INSERT INTO Grupo_Etnico (id, nombre) VALUES
  (1, 'Maya'),
  (2, 'Garífuna'),
  (3, 'Xinca'),
  (4, 'Mestizo / Ladino'),
  (5, 'No especificado');

-- ============================================================
-- Relacion  (familia del trabajador)
-- ============================================================
INSERT INTO Relacion (id, nombre) VALUES
  (1,  'Cónyuge'),
  (2,  'Hijo/a'),
  (3,  'Padre'),
  (4,  'Madre'),
  (5,  'Hermano/a'),
  (6,  'Abuelo/a'),
  (7,  'Nieto/a'),
  (8,  'Nuera / Yerno'),
  (9,  'Suegro/a'),
  (10, 'Otro');

-- ============================================================
-- Tipo_Institucion
-- ============================================================
INSERT INTO Tipo_Institucion (id, nombre) VALUES
  (1, 'Ministerio'),
  (2, 'Secretaría'),
  (3, 'Organismo del Estado'),
  (4, 'Entidad Autónoma'),
  (5, 'Entidad Descentralizada'),
  (6, 'Municipalidad'),
  (7, 'Otra');

-- ============================================================
-- Nivel_Institucion
-- ============================================================
INSERT INTO Nivel_Institucion (id, nombre) VALUES
  (1, 'Nacional'),
  (2, 'Departamental'),
  (3, 'Municipal'),
  (4, 'Regional');

-- ============================================================
-- Tipo_Empresa  (según clasificación RGAE)
-- ============================================================
INSERT INTO Tipo_Empresa (id, nombre) VALUES
  (1, 'Sociedad Anónima'),
  (2, 'Empresa Individual'),
  (3, 'Sociedad de Responsabilidad Limitada'),
  (4, 'Cooperativa'),
  (5, 'ONG / Asociación Civil'),
  (6, 'Consorcio');

-- ============================================================
-- Estado_Contrato
-- ============================================================
INSERT INTO Estado_Contrato (id, nombre) VALUES
  (1, 'Activo'),
  (2, 'Finalizado'),
  (3, 'Rescindido'),
  (4, 'Suspendido');

-- ============================================================
-- Tipo_Contrato  (OCDS + renglones presupuestarios)
-- NOTA: ids 1-2 son los métodos de contratación OCDS usados por
--       01_insert_contratos_ocds.sql; ids 3+ son renglones de RRHH.
-- ============================================================
INSERT INTO Tipo_Contrato (id, nombre) VALUES
  (1,  'open'),
  (2,  'limited'),
  (3,  'Renglon 011 - Personal Permanente'),
  (4,  'Renglon 022 - Personal por Contrato'),
  (5,  'Renglon 029 - Otras Remuneraciones'),
  (6,  'Renglon 031 - Jornales'),
  (7,  'Renglon 041 - Servicios Técnicos y Profesionales'),
  (8,  'Licitación Pública'),
  (9,  'Cotización'),
  (10, 'Compra Directa');

-- ============================================================
-- Tipo_Declaracion  (patrimonial)
-- ============================================================
INSERT INTO Tipo_Declaracion (id, nombre) VALUES
  (1, 'Inicial'),
  (2, 'Anual'),
  (3, 'Final');

-- ============================================================
-- Tipo_Pago
-- ============================================================
INSERT INTO Tipo_Pago (id, nombre) VALUES
  (1, 'Salario'),
  (2, 'Bonificación incentivo'),
  (3, 'Horas extra'),
  (4, 'Séptimo día'),
  (5, 'Aguinaldo'),
  (6, 'Bono 14'),
  (7, 'Indemnización'),
  (8, 'Otros');

-- ============================================================
-- Tipo_Denuncia
-- ============================================================
INSERT INTO Tipo_Denuncia (id, nombre) VALUES
  (1, 'Corrupción'),
  (2, 'Nepotismo'),
  (3, 'Abuso de autoridad'),
  (4, 'Enriquecimiento ilícito'),
  (5, 'Malversación de fondos'),
  (6, 'Otros');

-- ============================================================
-- Estado_Denuncia
-- ============================================================
INSERT INTO Estado_Denuncia (id, nombre) VALUES
  (1, 'Pendiente'),
  (2, 'En proceso'),
  (3, 'Resuelta'),
  (4, 'Archivada'),
  (5, 'Desestimada');

-- ============================================================
-- Tipo_Implicado
-- ============================================================
INSERT INTO Tipo_Implicado (id, nombre, descripcion) VALUES
  (1, 'Denunciado',  'Persona señalada como responsable en la denuncia'),
  (2, 'Testigo',     'Persona que presta declaración sobre los hechos'),
  (3, 'Víctima',     'Persona afectada directamente por los hechos'),
  (4, 'Cómplice',    'Persona que participó de forma secundaria');

SET FOREIGN_KEY_CHECKS = 1;
