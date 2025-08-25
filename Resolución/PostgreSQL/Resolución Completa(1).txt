-------------------------------------------------------DESAFIO: PARTE TÉCNICA---------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------CREACIÓN DE TABLAS-----------------------------------------------------------
CREATE TABLE desafio.caracteristics(
	num_acc TEXT PRIMARY KEY,
	an SMALLINT,
	mois SMALLINT,
	jour SMALLINT,
	hrmn SMALLINT,
	lum SMALLINT,
	agg SMALLINT,
	int SMALLINT,
	atm TEXT,
	col TEXT,
	com TEXT,
	adr TEXT,
	gps TEXT,
	lat TEXT,
	long TEXT,
	dep SMALLINT
);
--SELECT * FROM desafio.caracteristics;

CREATE TABLE desafio.users(
	id SERIAL PRIMARY KEY,
	num_acc TEXT,
	place TEXT,
	catu SMALLINT,
	grav SMALLINT,
	sexe SMALLINT,
	trajet TEXT,
	secu TEXT,
	locp TEXT,
	actp TEXT,
	etatp TEXT,
	an_nais TEXT,
	num_veh TEXT,
	grupo_etario SMALLINT
);
--SELECT * FROM desafio.users;

CREATE TABLE desafio.grupo_etario(
	id INTEGER PRIMARY KEY,
	descripcion TEXT
);
--SELECT * FROM desafio.grupo_etario;
---------------------------------------------------------------CARGA DE DATOS---------------------------------------------------------------

COPY desafio.caracteristics FROM 'C:\dataset_accidents_in_France\caracteristics.csv'  WITH (ENCODING 'LATIN9', FORMAT csv, DELIMITER ',', HEADER);
--SELECT * FROM desafio.caracteristics;

COPY desafio.users (num_acc,place,catu,grav,sexe,trajet,secu,locp,actp,etatp,an_nais,num_veh)FROM 'C:\dataset_accidents_in_France\users.csv'  WITH (ENCODING 'LATIN9', FORMAT csv, DELIMITER ',', HEADER);
--SELECT * FROM desafio.users;

INSERT INTO desafio.grupo_etario (id, descripcion) VALUES
(1, '0 a 15 años'),
(2, '16 a 30 años'),
(3, '31 a 45 años'),
(4, '46 a 60 años'),
(5, 'Mayores a 61 años');
--SELECT * FROM desafio.grupo_etario;
------------------------------------------------------------CREACIÓN DE CONSTRAINS-----------------------------------------------------------

--caracteristics:
CREATE INDEX depto ON desafio.caracteristics (dep); --Para mejorar la performance del punto D (se redujo 4 veces el tiempo de ejecución).

--users:
CREATE INDEX num_acc_users ON desafio.users (num_acc); --Para mejorar la performance del ORDER BY y JOINS

ALTER TABLE desafio.users 
ADD CONSTRAINT fk_num_acc
FOREIGN KEY (num_acc) 
REFERENCES desafio.caracteristics (num_acc);

ALTER TABLE desafio.users 
ADD CONSTRAINT fk_grupo_etario
FOREIGN KEY (grupo_etario) 
REFERENCES desafio.grupo_etario (id);
----------------------------------------------------------MODIFICACIÓN DE DATOS---------------------------------------------------------------

--caracteristics:
UPDATE desafio.caracteristics
SET an = an + 2000

--users:
BEGIN TRANSACTION; --VUELVO LOS 'NA' DE an_nais EN null.  
UPDATE desafio.users
SET an_nais = null
WHERE an_nais = 'NA'
COMMIT TRANSACTION;
ROLLBACK TRANSACTION;

CREATE PROCEDURE desafio.grupo_etario()
LANGUAGE plpgsql
AS $$
DECLARE
	cursor_ge CURSOR FOR 
			SELECT c.an, u.an_nais, u.id 
			FROM desafio.caracteristics c JOIN desafio.users u ON u.num_acc = c.num_acc;
	anio_acc SMALLINT;
	anio_nac TEXT;
	id_user INTEGER;
	edad SMALLINT;
BEGIN
	OPEN cursor_ge;
	LOOP
		FETCH NEXT FROM cursor_ge INTO anio_acc, anio_nac, id_user;
		EXIT WHEN NOT FOUND;
			IF (anio_nac is null) THEN
				BEGIN
					UPDATE desafio.users
					SET grupo_etario = 1
					WHERE id = id_user;
				END;
			ELSE
				BEGIN
					edad := anio_acc - CAST(anio_nac AS smallint);
					
					UPDATE desafio.users
					SET grupo_etario =
						CASE
							WHEN edad <= 15 THEN 1
							WHEN edad <= 30 THEN 2
							WHEN edad <= 45 THEN 3
							WHEN edad <= 60 THEN 4
							ELSE 5
						END
					WHERE id = id_user;
				END;
			END IF;
	END LOOP;
	CLOSE cursor_ge;
	--DEALLOCATE cursor_ge; me daba fallo al utlizar la rutina
END;
$$;

BEGIN TRANSACTION;   

CALL desafio.grupo_etario();

COMMIT TRANSACTION;
ROLLBACK TRANSACTION;

------------------------------------------------------------RESOLUCIÓN----------------------------------------------------------------------
--A

SELECT c.an, 
       COUNT(CASE WHEN u.sexe = 1 THEN u.num_acc END) "cant_acc_hombres",
       COUNT(CASE WHEN u.sexe = 2 THEN u.num_acc END) "cant_acc_mujeres"
FROM desafio.caracteristics c 
JOIN desafio.users u ON u.num_acc = c.num_acc
GROUP BY c.an;

--D
SELECT c.dep, u.grav, COUNT(u.num_acc) "cant_acc_grav",
										(
										SELECT COUNT(u1.num_acc)
										FROM desafio.users u1 JOIN desafio.caracteristics c1 ON u1.num_acc = c1.num_acc
										WHERE c1.dep = c.dep
										GROUP BY c1.dep
										) "cant_acc_depto"
FROM desafio.caracteristics c JOIN desafio.users u ON (c.num_acc = u.num_acc)
GROUP BY u.grav, c.dep
ORDER BY 4 DESC,3 DESC
LIMIT 16;

--F
SELECT u.grupo_etario "id_grupo_etario",
		g.descripcion "descripcion_grupo_etario",
		COUNT(CASE WHEN sexe = 1 THEN num_acc END) "cant_acc_hombres",
    	COUNT(CASE WHEN sexe = 2 THEN num_acc END) "cant_acc_mujeres"
FROM desafio.users u JOIN desafio.grupo_etario g ON g.id = u.grupo_etario
GROUP BY u.grupo_etario, g.descripcion
ORDER BY 1;







