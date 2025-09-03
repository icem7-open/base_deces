-- SCRIPT DE GENERATION D'UNE BASE NATIONALE DES DECES NETTOYEE - COMPATIBLE DUCKDB

-- 1 Fonctions de retraitement
-- produit une date valide (typée date)
CREATE OR REPLACE MACRO corrige_date(s) AS
	CASE WHEN s[1:4] BETWEEN '0100' AND '1830' OR s[1:4] > '2030' THEN NULL
	WHEN try_strptime(s, '%Y%m%d') IS NOT NULL THEN try_strptime(s, '%Y%m%d') 
	WHEN s = '00000000' OR try_cast(s[5:6] AS INT) > 12 THEN NULL::date
	WHEN s[-4:] = '0000' THEN strptime(s[1:4] || '-07-01', '%Y-%m-%d')
	WHEN s[-2:] = '00' THEN strptime(s[1:4] || '-' || s[5:6] || '-15', '%Y-%m-%d')
	ELSE NULL
	END ;

-- caractérise la correction apportée pour produire une date valide
-- 0 : date précise, 1 et 2 : date floue, 9 : date invalide ou inconnue
CREATE OR REPLACE MACRO flou_date(s) AS
	CASE WHEN s[1:4] BETWEEN '0100' AND '1830' OR s[1:4] > '2030' THEN 9
	WHEN try_strptime(s, '%Y%m%d') IS NOT NULL THEN 0 
	WHEN s = '00000000' OR try_cast(s[5:6] AS INT) > 12 THEN 9
	WHEN s[-4:] = '0000' THEN 2
	WHEN s[-2:] = '00' THEN 1
	ELSE 9
	END ;

-- produit un pays_naissance simplifié
CREATE OR REPLACE MACRO corrige_pays(p, c) AS
	CASE WHEN p IN ('LA REUNION','REUNION（LA）') THEN 'REUNION'
	WHEN p IN ('ETATS UNIS D''AMERIQUE', 'ETATS UNIS D AMERIQUE', 'ETATS-UNIS D''AMERIQUE', 'USA') THEN 'ETATS UNIS'
	WHEN p IN ('VIET NAM', 'VIET-NAM', 'VIET NAM DU SUD','SUD VIETNAM','VIET NAM DU NORD', 'NORD VIETNAM', 'NORD VIET NAM') THEN 'VIETNAM'
	WHEN p IN ('GRANDE BRETAGNE', 'ANGLETERRE', 'ROYAUME UNI') THEN 'ROYAUME-UNI'
	WHEN p IN ('HOLLANDE') THEN 'PAYS BAS'
	WHEN p IN ('RFA') THEN 'ALLEMAGNE'
	WHEN p IN ('ILE MAURICE') THEN 'MAURICE'
	WHEN c IN ('91352','92352','93352','94352','99352') THEN 'ALGERIE' 
	WHEN p = '' AND c like '99%' THEN 'AUTRES ÉTRANGER' 
	WHEN p = '' THEN 'FRANCE OU ANCIENS TERRITOIRES FRANÇAIS'
	ELSE p END ;


-- 2 Liste des URLs à lire sur le dépôt https://www.data.gouv.fr/datasets/fichier-des-personnes-decedees/ 
-- pointent vers des fichiers annuels, ou mensuels pour l'année en cours
CREATE OR REPLACE TABLE urls_deces AS 
	WITH t1 AS ( 
		FROM read_json('https://www.data.gouv.fr/api/1/datasets/5de8f397634f4164071119c5/')
		SELECT url: unnest(resources->>'$[*].url') 
	), t2 AS (
		FROM t1 
		SELECT lastfullyear: max(regexp_extract(url, '.*deces-(\d{4}).txt',1)) -- dernière année entière
		WHERE url ~ '.*deces-\d{4}.txt'
	) 
	FROM t1, t2 
	SELECT  url,
			an: regexp_extract(url, '.*deces-(\d{4}).*.txt',1)
	WHERE url ~ '.*deces-\d{4}.txt'   -- années complètes
	OR (url ~ '.*deces-\d{4}-m\d{2}.txt'	
		AND an > t2.lastfullyear) 	  -- les mois de l'année en cours
;

-- constitution de deux sous listes d'urls, avant et après 2017 (encodages différents à gérer)
SET variable URLS1 = (
	FROM urls_deces
	SELECT list(url)
	WHERE an >= '2017'
) ;

SET variable URLS2 = (
	FROM urls_deces
	SELECT list(url)
	WHERE an < '2017'
) ;


-- 3 Lecture groupée de tous les fichiers, décodage de chaque enregistrement + dédoublonnage
LOAD encodings ; 

COPY (
	WITH t1 AS (
		FROM read_csv(getvariable('URLS1'), encoding = 'utf-8', 	-- les fichiers de 2017 et après sont en utf-8
						strict_mode = FALSE, union_by_name = TRUE,
					 	header = FALSE, COLUMNS = {'record': 'VARCHAR'}, filename = TRUE)
		UNION ALL
		(FROM read_csv(getvariable('URLS2'), encoding = '8859_1', 	-- les fichiers avant 2017 sont en 8859_1
						strict_mode = FALSE, union_by_name = TRUE,
					 	header = FALSE, COLUMNS = {'record': 'VARCHAR'}, filename = TRUE))
	), t2 AS (
		FROM t1
		SELECT
			nom: 				trim(record[1:80], ' /').split_part('*',1), 
		    prenoms: 			trim(record[1:80], ' /').split_part('*',2).replace(' ', ', '),
		    sexe: 				if(record[81] = '1','M','F'),
		    date_naissance0: 	(trim(record[82:89]) || '0000')[1:8],
		    flou_date_naissance: flou_date(date_naissance0),
		    code_insee_naissance: record[90:94],
		    commune_naissance: 	trim(record[95:124]),
		    pays_naissance0: 	trim(record[125:154]),
		    pays_naissance: 	corrige_pays(pays_naissance0, code_insee_naissance),  
		    date_deces0: 		(trim(record[155:162]) || '0000')[1:8],
		    code_insee_deces: 	record[163:167],
		    numero_acte_deces: 	trim(record[168:176]),
		    fichier_origine: 	regexp_extract(filename, '.*deces-(\d{4}).*.txt',1),
		    date_naissance: 	corrige_date(date_naissance0),
			date_deces:			corrige_date(date_deces0),
			age: 				round(date_diff('month', date_naissance, date_deces) / 12, 1)
	) FROM t2 
		SELECT DISTINCT ON (date_deces0,code_insee_deces,nom,prenoms) * -- dédoublonnage
) 
TO 'tmp_deces.parquet' ; -- fichier parquet intermédiaire
-- 5'


-- 4 tri du fichier et écriture finale
-- récupération des oppositions
SET variable URL_OPPOSITION = (
	WITH t1 AS ( 
		FROM read_json('https://www.data.gouv.fr/api/1/datasets/5de8f397634f4164071119c5/')
		SELECT url: unnest(resources->>'$[*].url')
	) FROM t1 
	SELECT url
	WHERE url LIKE '%opposition%' LIMIT 1
);

-- élimination des oppositions et tri avant export parquet optimisé
-- le tri améliore la compression
COPY (
	WITH t1 AS (
		FROM read_csv(getvariable('URL_OPPOSITION'))
		SELECT DISTINCT *  -- il y a des doublons dans ce fichier d'oppositions !
		RENAME("Date de décès" AS date_deces0, 
		"Code du lieu de décès" AS code_insee_deces, 
		"Numéro d'acte de décès" AS numero_acte_deces )
	) 
	FROM 'tmp_deces.parquet' dc  
	ANTI JOIN t1 USING(date_deces0,code_insee_deces,numero_acte_deces)
	ORDER BY fichier_origine, pays_naissance, code_insee_naissance, date_deces, nom
) 
TO 'base_deces.parquet' (COMPRESSION zstd, parquet_version v2);
-- 1'


-- 5 réduction du fichier intermédiaire, à défaut de pouvoir le supprimer
COPY (values(1)) TO 'tmp_deces.parquet' ;



