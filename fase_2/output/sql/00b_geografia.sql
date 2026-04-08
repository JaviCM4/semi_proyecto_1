-- ============================================================
-- 00b_geografia.sql
-- Departamentos y municipios de Guatemala (335 municipios)
-- Ejecutar DESPUÉS de 00_catalogos_base.sql (depende de Pais id=1)
-- ============================================================

USE ods_justicia;
SET FOREIGN_KEY_CHECKS = 0;

-- ============================================================
-- Departamentos (22)
-- ============================================================
INSERT INTO Departamento (id, id_pais, nombre) VALUES
  ( 1, 1, 'Guatemala'),
  ( 2, 1, 'El Progreso'),
  ( 3, 1, 'Sacatepéquez'),
  ( 4, 1, 'Chimaltenango'),
  ( 5, 1, 'Escuintla'),
  ( 6, 1, 'Santa Rosa'),
  ( 7, 1, 'Sololá'),
  ( 8, 1, 'Totonicapán'),
  ( 9, 1, 'Quetzaltenango'),
  (10, 1, 'Suchitepéquez'),
  (11, 1, 'Retalhuleu'),
  (12, 1, 'San Marcos'),
  (13, 1, 'Huehuetenango'),
  (14, 1, 'El Quiché'),
  (15, 1, 'Baja Verapaz'),
  (16, 1, 'Alta Verapaz'),
  (17, 1, 'Petén'),
  (18, 1, 'Izabal'),
  (19, 1, 'Zacapa'),
  (20, 1, 'Chiquimula'),
  (21, 1, 'Jalapa'),
  (22, 1, 'Jutiapa');

-- ============================================================
-- Municipios
-- ============================================================

-- ------------------------------------------------------------
-- Guatemala (id_departamento = 1) — 17 municipios  (ids 1-17)
-- ------------------------------------------------------------
INSERT INTO Municipio (id, id_departamento, nombre) VALUES
  ( 1, 1, 'Guatemala'),
  ( 2, 1, 'Santa Catarina Pínula'),
  ( 3, 1, 'San José Pínula'),
  ( 4, 1, 'San José del Golfo'),
  ( 5, 1, 'Palencia'),
  ( 6, 1, 'Chinautla'),
  ( 7, 1, 'San Pedro Ayampuc'),
  ( 8, 1, 'Mixco'),
  ( 9, 1, 'San Pedro Sacatepéquez'),
  (10, 1, 'San Juan Sacatepéquez'),
  (11, 1, 'San Raymundo'),
  (12, 1, 'Chuarrancho'),
  (13, 1, 'Fraijanes'),
  (14, 1, 'Amatitlán'),
  (15, 1, 'Villa Nueva'),
  (16, 1, 'Villa Canales'),
  (17, 1, 'San Miguel Petapa');

-- ------------------------------------------------------------
-- El Progreso (id_departamento = 2) — 8 municipios  (ids 18-25)
-- ------------------------------------------------------------
INSERT INTO Municipio (id, id_departamento, nombre) VALUES
  (18, 2, 'Guastatoya'),
  (19, 2, 'Morazán'),
  (20, 2, 'San Agustín Acasaguastlán'),
  (21, 2, 'San Cristóbal Acasaguastlán'),
  (22, 2, 'El Jícaro'),
  (23, 2, 'Sansare'),
  (24, 2, 'Sanarate'),
  (25, 2, 'San Antonio La Paz');

-- ------------------------------------------------------------
-- Sacatepéquez (id_departamento = 3) — 16 municipios  (ids 26-41)
-- ------------------------------------------------------------
INSERT INTO Municipio (id, id_departamento, nombre) VALUES
  (26, 3, 'Antigua Guatemala'),
  (27, 3, 'Jocotenango'),
  (28, 3, 'Pastores'),
  (29, 3, 'Sumpango'),
  (30, 3, 'Santo Domingo Xenacoj'),
  (31, 3, 'Santiago Sacatepéquez'),
  (32, 3, 'San Bartolomé Milpas Altas'),
  (33, 3, 'San Lucas Sacatepéquez'),
  (34, 3, 'Santa Lucía Milpas Altas'),
  (35, 3, 'Magdalena Milpas Altas'),
  (36, 3, 'Santa María de Jesús'),
  (37, 3, 'Ciudad Vieja'),
  (38, 3, 'San Miguel Dueñas'),
  (39, 3, 'Alotenango'),
  (40, 3, 'San Antonio Aguas Calientes'),
  (41, 3, 'Santa Catarina Barahona');

-- ------------------------------------------------------------
-- Chimaltenango (id_departamento = 4) — 16 municipios  (ids 42-57)
-- ------------------------------------------------------------
INSERT INTO Municipio (id, id_departamento, nombre) VALUES
  (42, 4, 'Chimaltenango'),
  (43, 4, 'San José Poaquil'),
  (44, 4, 'San Martín Jilotepeque'),
  (45, 4, 'San Juan Comalapa'),
  (46, 4, 'Santa Apolonia'),
  (47, 4, 'Tecpán Guatemala'),
  (48, 4, 'Patzún'),
  (49, 4, 'Pochuta'),
  (50, 4, 'Patzicía'),
  (51, 4, 'Santa Cruz Balanyá'),
  (52, 4, 'Acatenango'),
  (53, 4, 'Yepocapa'),
  (54, 4, 'San Andrés Itzapa'),
  (55, 4, 'Parramos'),
  (56, 4, 'Zaragoza'),
  (57, 4, 'El Tejar');

-- ------------------------------------------------------------
-- Escuintla (id_departamento = 5) — 13 municipios  (ids 58-70)
-- ------------------------------------------------------------
INSERT INTO Municipio (id, id_departamento, nombre) VALUES
  (58, 5, 'Escuintla'),
  (59, 5, 'Santa Lucía Cotzumalguapa'),
  (60, 5, 'La Democracia'),
  (61, 5, 'Siquinalá'),
  (62, 5, 'Masagua'),
  (63, 5, 'Tiquisate'),
  (64, 5, 'La Gomera'),
  (65, 5, 'Guanagazapa'),
  (66, 5, 'San José'),
  (67, 5, 'Iztapa'),
  (68, 5, 'Palín'),
  (69, 5, 'San Vicente Pacaya'),
  (70, 5, 'Nueva Concepción');

-- ------------------------------------------------------------
-- Santa Rosa (id_departamento = 6) — 14 municipios  (ids 71-84)
-- ------------------------------------------------------------
INSERT INTO Municipio (id, id_departamento, nombre) VALUES
  (71, 6, 'Cuilapa'),
  (72, 6, 'Barberena'),
  (73, 6, 'Santa Rosa de Lima'),
  (74, 6, 'Casillas'),
  (75, 6, 'San Rafael Las Flores'),
  (76, 6, 'Oratorio'),
  (77, 6, 'San Juan Tecuaco'),
  (78, 6, 'Chiquimulilla'),
  (79, 6, 'Taxisco'),
  (80, 6, 'Santa María Ixhuatán'),
  (81, 6, 'Guazacapán'),
  (82, 6, 'Santa Cruz Naranjo'),
  (83, 6, 'Pueblo Nuevo Viñas'),
  (84, 6, 'Nueva Santa Rosa');

-- ------------------------------------------------------------
-- Sololá (id_departamento = 7) — 19 municipios  (ids 85-103)
-- ------------------------------------------------------------
INSERT INTO Municipio (id, id_departamento, nombre) VALUES
  ( 85, 7, 'Sololá'),
  ( 86, 7, 'San José Chacayá'),
  ( 87, 7, 'Santa María Visitación'),
  ( 88, 7, 'Santa Lucía Utatlán'),
  ( 89, 7, 'Nahualá'),
  ( 90, 7, 'Santa Catarina Ixtahuacán'),
  ( 91, 7, 'Santa Clara La Laguna'),
  ( 92, 7, 'Concepción'),
  ( 93, 7, 'San Andrés Semetabaj'),
  ( 94, 7, 'Panajachel'),
  ( 95, 7, 'Santa Catarina Palopó'),
  ( 96, 7, 'San Antonio Palopó'),
  ( 97, 7, 'San Lucas Tolimán'),
  ( 98, 7, 'Santa Cruz La Laguna'),
  ( 99, 7, 'San Pablo La Laguna'),
  (100, 7, 'San Marcos La Laguna'),
  (101, 7, 'San Juan La Laguna'),
  (102, 7, 'San Pedro La Laguna'),
  (103, 7, 'Santiago Atitlán');

-- ------------------------------------------------------------
-- Totonicapán (id_departamento = 8) — 8 municipios  (ids 104-111)
-- ------------------------------------------------------------
INSERT INTO Municipio (id, id_departamento, nombre) VALUES
  (104, 8, 'Totonicapán'),
  (105, 8, 'San Cristóbal Totonicapán'),
  (106, 8, 'San Francisco El Alto'),
  (107, 8, 'San Andrés Xecul'),
  (108, 8, 'Momostenango'),
  (109, 8, 'Santa María Chiquimula'),
  (110, 8, 'Santa Lucía La Reforma'),
  (111, 8, 'San Bartolo Aguas Calientes');

-- ------------------------------------------------------------
-- Quetzaltenango (id_departamento = 9) — 24 municipios  (ids 112-135)
-- ------------------------------------------------------------
INSERT INTO Municipio (id, id_departamento, nombre) VALUES
  (112,  9, 'Quetzaltenango'),
  (113,  9, 'Salcajá'),
  (114,  9, 'Olintepeque'),
  (115,  9, 'San Carlos Sija'),
  (116,  9, 'Sibilia'),
  (117,  9, 'Cabricán'),
  (118,  9, 'Cajolá'),
  (119,  9, 'San Miguel Sigüilá'),
  (120,  9, 'San Juan Ostuncalco'),
  (121,  9, 'San Mateo'),
  (122,  9, 'Concepción Chiquirichapa'),
  (123,  9, 'San Martín Sacatepéquez'),
  (124,  9, 'Almolonga'),
  (125,  9, 'Cantel'),
  (126,  9, 'Huitán'),
  (127,  9, 'Zunil'),
  (128,  9, 'Colomba Costa Cuca'),
  (129,  9, 'San Francisco La Unión'),
  (130,  9, 'El Palmar'),
  (131,  9, 'Coatepeque'),
  (132,  9, 'Génova'),
  (133,  9, 'Flores Costa Cuca'),
  (134,  9, 'La Esperanza'),
  (135,  9, 'Palestina de Los Altos');

-- ------------------------------------------------------------
-- Suchitepéquez (id_departamento = 10) — 21 municipios  (ids 136-156)
-- ------------------------------------------------------------
INSERT INTO Municipio (id, id_departamento, nombre) VALUES
  (136, 10, 'Mazatenango'),
  (137, 10, 'Cuyotenango'),
  (138, 10, 'San Francisco Zapotitlán'),
  (139, 10, 'San Bernardino'),
  (140, 10, 'San José El Ídolo'),
  (141, 10, 'Santo Domingo Suchitepéquez'),
  (142, 10, 'San Lorenzo'),
  (143, 10, 'Samayac'),
  (144, 10, 'San Pablo Jocopilas'),
  (145, 10, 'San Antonio Suchitepéquez'),
  (146, 10, 'San Miguel Panán'),
  (147, 10, 'San Gabriel'),
  (148, 10, 'Chicacao'),
  (149, 10, 'Patulul'),
  (150, 10, 'Santa Bárbara'),
  (151, 10, 'San Juan Bautista'),
  (152, 10, 'Santo Tomás La Unión'),
  (153, 10, 'Zunilito'),
  (154, 10, 'Pueblo Nuevo'),
  (155, 10, 'Río Bravo'),
  (156, 10, 'San José La Máquina');

-- ------------------------------------------------------------
-- Retalhuleu (id_departamento = 11) — 9 municipios  (ids 157-165)
-- ------------------------------------------------------------
INSERT INTO Municipio (id, id_departamento, nombre) VALUES
  (157, 11, 'Retalhuleu'),
  (158, 11, 'San Sebastián'),
  (159, 11, 'Santa Cruz Muluá'),
  (160, 11, 'San Martín Zapotitlán'),
  (161, 11, 'San Felipe'),
  (162, 11, 'San Andrés Villa Seca'),
  (163, 11, 'Champerico'),
  (164, 11, 'Nuevo San Carlos'),
  (165, 11, 'El Asintal');

-- ------------------------------------------------------------
-- San Marcos (id_departamento = 12) — 30 municipios  (ids 166-195)
-- ------------------------------------------------------------
INSERT INTO Municipio (id, id_departamento, nombre) VALUES
  (166, 12, 'San Marcos'),
  (167, 12, 'San Pedro Sacatepéquez'),
  (168, 12, 'San Antonio Sacatepéquez'),
  (169, 12, 'Comitancillo'),
  (170, 12, 'San Miguel Ixtahuacán'),
  (171, 12, 'Concepción Tutuapa'),
  (172, 12, 'Tacaná'),
  (173, 12, 'Sibinal'),
  (174, 12, 'Tajumulco'),
  (175, 12, 'Tejutla'),
  (176, 12, 'San Rafael Pie de la Cuesta'),
  (177, 12, 'Nuevo Progreso'),
  (178, 12, 'El Tumbador'),
  (179, 12, 'El Rodeo'),
  (180, 12, 'Malacatán'),
  (181, 12, 'Catarina'),
  (182, 12, 'Ayutla'),
  (183, 12, 'Ocós'),
  (184, 12, 'San Pablo'),
  (185, 12, 'El Quetzal'),
  (186, 12, 'La Reforma'),
  (187, 12, 'Pajapita'),
  (188, 12, 'Ixchiguán'),
  (189, 12, 'San José Ojetenam'),
  (190, 12, 'San Cristóbal Cucho'),
  (191, 12, 'Sipacapa'),
  (192, 12, 'Esquipulas Palo Gordo'),
  (193, 12, 'Río Blanco'),
  (194, 12, 'San Lorenzo'),
  (195, 12, 'La Blanca');

-- ------------------------------------------------------------
-- Huehuetenango (id_departamento = 13) — 32 municipios  (ids 196-227)
-- ------------------------------------------------------------
INSERT INTO Municipio (id, id_departamento, nombre) VALUES
  (196, 13, 'Huehuetenango'),
  (197, 13, 'Chiantla'),
  (198, 13, 'Malacatancito'),
  (199, 13, 'Cuilco'),
  (200, 13, 'Nentón'),
  (201, 13, 'San Pedro Necta'),
  (202, 13, 'Jacaltenango'),
  (203, 13, 'Soloma'),
  (204, 13, 'Ixtahuacán'),
  (205, 13, 'Santa Bárbara'),
  (206, 13, 'La Libertad'),
  (207, 13, 'La Democracia'),
  (208, 13, 'San Miguel Acatán'),
  (209, 13, 'San Rafael La Independencia'),
  (210, 13, 'Todos Santos Cuchumatán'),
  (211, 13, 'San Juan Atitán'),
  (212, 13, 'Santa Eulalia'),
  (213, 13, 'San Mateo Ixtatán'),
  (214, 13, 'Colotenango'),
  (215, 13, 'San Sebastián Huehuetenango'),
  (216, 13, 'Tectitán'),
  (217, 13, 'Concepción Huista'),
  (218, 13, 'San Juan Ixcoy'),
  (219, 13, 'San Antonio Huista'),
  (220, 13, 'San Sebastián Coatán'),
  (221, 13, 'Barillas'),
  (222, 13, 'Aguacatán'),
  (223, 13, 'San Rafael Petzal'),
  (224, 13, 'San Gaspar Ixchil'),
  (225, 13, 'Santiago Chimaltenango'),
  (226, 13, 'Santa Ana Huista'),
  (227, 13, 'Unión Cantinil');

-- ------------------------------------------------------------
-- El Quiché (id_departamento = 14) — 21 municipios  (ids 228-248)
-- ------------------------------------------------------------
INSERT INTO Municipio (id, id_departamento, nombre) VALUES
  (228, 14, 'Santa Cruz del Quiché'),
  (229, 14, 'Chiché'),
  (230, 14, 'Chinique'),
  (231, 14, 'Zacualpa'),
  (232, 14, 'Chajul'),
  (233, 14, 'Chichicastenango'),
  (234, 14, 'Patzité'),
  (235, 14, 'San Antonio Ilotenango'),
  (236, 14, 'San Pedro Jocopilas'),
  (237, 14, 'Cunén'),
  (238, 14, 'San Juan Cotzal'),
  (239, 14, 'Joyabaj'),
  (240, 14, 'Nebaj'),
  (241, 14, 'San Andrés Sajcabajá'),
  (242, 14, 'Uspantán'),
  (243, 14, 'Sacapulas'),
  (244, 14, 'San Bartolomé Jocotenango'),
  (245, 14, 'Canillá'),
  (246, 14, 'Chicamán'),
  (247, 14, 'Ixcán'),
  (248, 14, 'Pachalum');

-- ------------------------------------------------------------
-- Baja Verapaz (id_departamento = 15) — 8 municipios  (ids 249-256)
-- ------------------------------------------------------------
INSERT INTO Municipio (id, id_departamento, nombre) VALUES
  (249, 15, 'Salamá'),
  (250, 15, 'San Miguel Chicaj'),
  (251, 15, 'Rabinal'),
  (252, 15, 'Cubulco'),
  (253, 15, 'Granados'),
  (254, 15, 'Santa Cruz El Chol'),
  (255, 15, 'San Jerónimo'),
  (256, 15, 'Purulhá');

-- ------------------------------------------------------------
-- Alta Verapaz (id_departamento = 16) — 17 municipios  (ids 257-273)
-- ------------------------------------------------------------
INSERT INTO Municipio (id, id_departamento, nombre) VALUES
  (257, 16, 'Cobán'),
  (258, 16, 'Santa Cruz Verapaz'),
  (259, 16, 'San Cristóbal Verapaz'),
  (260, 16, 'Tactic'),
  (261, 16, 'Tamahú'),
  (262, 16, 'San Miguel Tucurú'),
  (263, 16, 'Panzós'),
  (264, 16, 'Senahú'),
  (265, 16, 'San Pedro Carchá'),
  (266, 16, 'San Juan Chamelco'),
  (267, 16, 'Lanquín'),
  (268, 16, 'Cahabón'),
  (269, 16, 'Chisec'),
  (270, 16, 'Chahal'),
  (271, 16, 'Fray Bartolomé de las Casas'),
  (272, 16, 'Santa Catalina La Tinta'),
  (273, 16, 'Raxruhá');

-- ------------------------------------------------------------
-- Petén (id_departamento = 17) — 12 municipios  (ids 274-285)
-- ------------------------------------------------------------
INSERT INTO Municipio (id, id_departamento, nombre) VALUES
  (274, 17, 'Flores'),
  (275, 17, 'San José'),
  (276, 17, 'San Benito'),
  (277, 17, 'San Andrés'),
  (278, 17, 'La Libertad'),
  (279, 17, 'San Francisco'),
  (280, 17, 'Santa Ana'),
  (281, 17, 'Dolores'),
  (282, 17, 'San Luis'),
  (283, 17, 'Sayaxché'),
  (284, 17, 'Melchor de Mencos'),
  (285, 17, 'Poptún');

-- ------------------------------------------------------------
-- Izabal (id_departamento = 18) — 5 municipios  (ids 286-290)
-- ------------------------------------------------------------
INSERT INTO Municipio (id, id_departamento, nombre) VALUES
  (286, 18, 'Puerto Barrios'),
  (287, 18, 'Livingston'),
  (288, 18, 'El Estor'),
  (289, 18, 'Morales'),
  (290, 18, 'Los Amates');

-- ------------------------------------------------------------
-- Zacapa (id_departamento = 19) — 11 municipios  (ids 291-301)
-- ------------------------------------------------------------
INSERT INTO Municipio (id, id_departamento, nombre) VALUES
  (291, 19, 'Zacapa'),
  (292, 19, 'Estanzuela'),
  (293, 19, 'Río Hondo'),
  (294, 19, 'Gualán'),
  (295, 19, 'Teculután'),
  (296, 19, 'Usumatlán'),
  (297, 19, 'Cabañas'),
  (298, 19, 'San Diego'),
  (299, 19, 'La Unión'),
  (300, 19, 'Huité'),
  (301, 19, 'San Jorge');

-- ------------------------------------------------------------
-- Chiquimula (id_departamento = 20) — 11 municipios  (ids 302-312)
-- ------------------------------------------------------------
INSERT INTO Municipio (id, id_departamento, nombre) VALUES
  (302, 20, 'Chiquimula'),
  (303, 20, 'San José La Arada'),
  (304, 20, 'San Juan Ermita'),
  (305, 20, 'Jocotán'),
  (306, 20, 'Camotán'),
  (307, 20, 'Olopa'),
  (308, 20, 'Esquipulas'),
  (309, 20, 'Concepción Las Minas'),
  (310, 20, 'Quezaltepeque'),
  (311, 20, 'San Jacinto'),
  (312, 20, 'Ipala');

-- ------------------------------------------------------------
-- Jalapa (id_departamento = 21) — 7 municipios  (ids 313-319)
-- ------------------------------------------------------------
INSERT INTO Municipio (id, id_departamento, nombre) VALUES
  (313, 21, 'Jalapa'),
  (314, 21, 'San Pedro Pinula'),
  (315, 21, 'San Luis Jilotepeque'),
  (316, 21, 'San Manuel Chaparrón'),
  (317, 21, 'San Carlos Alzatate'),
  (318, 21, 'Monjas'),
  (319, 21, 'Mataquescuintla');

-- ------------------------------------------------------------
-- Jutiapa (id_departamento = 22) — 17 municipios  (ids 320-336)
-- ------------------------------------------------------------
INSERT INTO Municipio (id, id_departamento, nombre) VALUES
  (320, 22, 'Jutiapa'),
  (321, 22, 'El Progreso'),
  (322, 22, 'Santa Catarina Mita'),
  (323, 22, 'Agua Blanca'),
  (324, 22, 'Asunción Mita'),
  (325, 22, 'Yupiltepeque'),
  (326, 22, 'Atescatempa'),
  (327, 22, 'Jerez'),
  (328, 22, 'El Adelanto'),
  (329, 22, 'Zapotitlán'),
  (330, 22, 'Comapa'),
  (331, 22, 'Jalpatagua'),
  (332, 22, 'Conguaco'),
  (333, 22, 'Moyuta'),
  (334, 22, 'Pasaco'),
  (335, 22, 'San José Acatempa'),
  (336, 22, 'Quesada');

SET FOREIGN_KEY_CHECKS = 1;
