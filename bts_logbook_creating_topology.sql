DROP DATABASE IF EXISTS bts_logbook;
CREATE DATABASE bts_logbook;
USE bts_logbook;

DROP TABLE IF EXISTS regions;
CREATE TABLE regions(
	id SERIAL,
	name VARCHAR(200) NOT NULL,
	
	PRIMARY KEY (name)
);


DROP TABLE IF EXISTS bts_list;
CREATE TABLE bts_list(
	id SERIAL,
	region_id BIGINT UNSIGNED NOT NULL,
	bts_number INT UNSIGNED NOT NULL,
	name VARCHAR(100) AS (CONCAT('BTS_', region_id, '_', bts_number)) COMMENT "typical BTS name is BTS_<region_id>_<bts_number>",
	status VARCHAR(1) COMMENT "t - in work, f - inactive",
	
	INDEX bts_list_name_idx (name),
	FOREIGN KEY (region_id) REFERENCES regions(id)
) COMMENT="List of BTSs (base transceiver station) for some mobile operator";


DROP TABLE IF EXISTS cell_designer;
CREATE TABLE cell_designer(
	id SERIAL,
	first_last_name VARCHAR(200),
	region_id BIGINT UNSIGNED NOT NULL,
	
	INDEX cell_designer_name_idx(first_last_name),
	FOREIGN KEY (region_id) REFERENCES regions(id)
);

DROP TABLE IF EXISTS cell_types;
CREATE TABLE cell_types(
	id SERIAL,
	name VARCHAR(50) NOT NULL DEFAULT '2G',
	
	INDEX cell_types_type(name)
);

DROP TABLE IF EXISTS cell_list;
CREATE TABLE cell_list(
	id SERIAL,
	cell_number TINYINT UNSIGNED NOT NULL COMMENT "often from 1 to 3",
	designer_id BIGINT UNSIGNED NOT NULL,
	bts_id BIGINT UNSIGNED NOT NULL,
	cell_type_id BIGINT UNSIGNED NOT NULL,
	status VARCHAR(1) COMMENT "t - in work, f - inactive",
	
	FOREIGN KEY (bts_id) REFERENCES bts_list(id),
	FOREIGN KEY (cell_type_id) REFERENCES cell_types(id),
	FOREIGN KEY (designer_id) REFERENCES cell_designer(id)
);



DROP TABLE IF EXISTS bts_cell_operations;
CREATE TABLE bts_cell_operations(
	id SERIAL,
	cell_id BIGINT UNSIGNED NOT NULL,
	operation_type ENUM('commissioning', 'modification', 'dismantling'),
	operacion_date DATETIME DEFAULT NOW(),
	comment TEXT,
	
	FOREIGN KEY (cell_id) REFERENCES cell_list(id)
) COMMENT="All works with BTSs distributed by cells";


DROP TABLE IF EXISTS stat_2g;
CREATE TABLE stat_2g(
	cell_id BIGINT UNSIGNED NOT NULL,
	stime DATETIME NOT NULL,
	available_time BIGINT UNSIGNED COMMENT "value can be nullable, dimension in seconds",
	time_in_period BIGINT UNSIGNED,
	availability FLOAT AS (CASE WHEN (time_in_period <> 0) THEN (available_time / time_in_period) ELSE 0 END),
	ul_traf FLOAT,
	dl_traf FLOAT,
	sum_traf FLOAT AS (COALESCE(ul_traf, 0) + COALESCE(dl_traf, 0)),
	
	FOREIGN KEY (cell_id) REFERENCES cell_list(id)
);


DROP TABLE IF EXISTS stat_3g;
CREATE TABLE stat_3g(
	cell_id BIGINT UNSIGNED NOT NULL,
	stime DATETIME NOT NULL,
	available_time BIGINT UNSIGNED COMMENT "value can be nullable, dimension in seconds",
	time_in_period BIGINT UNSIGNED,
	availability FLOAT AS (CASE WHEN (time_in_period <> 0) THEN (available_time / time_in_period) ELSE 0 END),
	ul_traf FLOAT,
	dl_traf FLOAT,
	sum_traf FLOAT AS (COALESCE(ul_traf, 0) + COALESCE(dl_traf, 0)),
	
	FOREIGN KEY (cell_id) REFERENCES cell_list(id)
);


DROP TABLE IF EXISTS stat_4g;
CREATE TABLE stat_4g(
	cell_id BIGINT UNSIGNED NOT NULL,
	stime DATETIME NOT NULL,
	available_time BIGINT UNSIGNED COMMENT "value can be nullable, dimension in seconds",
	time_in_period BIGINT UNSIGNED,
	availability FLOAT AS (CASE WHEN (time_in_period > 0) THEN (available_time / time_in_period) ELSE 0 END),
	ul_traf FLOAT,
	dl_traf FLOAT,
	sum_traf FLOAT AS (COALESCE(ul_traf, 0) + COALESCE(dl_traf, 0)),
	
	FOREIGN KEY (cell_id) REFERENCES cell_list(id)
);

DROP TABLE IF EXISTS stat_other;
CREATE TABLE stat_other(
	cell_id BIGINT UNSIGNED NOT NULL,
	stime DATETIME NOT NULL,
	available_time BIGINT UNSIGNED COMMENT "value can be nullable, dimension in seconds",
	time_in_period BIGINT UNSIGNED,
	availability FLOAT AS (CASE WHEN (time_in_period > 0) THEN (available_time / time_in_period) ELSE 0 END),
	ul_traf FLOAT,
	dl_traf FLOAT,
	sum_traf FLOAT AS (COALESCE(ul_traf, 0) + COALESCE(dl_traf, 0)),
	
	FOREIGN KEY (cell_id) REFERENCES cell_list(id)
);


DROP TABLE IF EXISTS calls_by_cell;
CREATE TABLE calls_by_cell(
	id SERIAL,
	cell_id BIGINT UNSIGNED NOT NULL,
	start_time TIMESTAMP DEFAULT NOW(),
	originate_number BIGINT UNSIGNED NOT NULL COMMENT "calling subscriber",
	terminate_number BIGINT UNSIGNED NOT NULL COMMENT "called subscriber",
	call_duration INT UNSIGNED NOT NULL,
	
	FOREIGN KEY (cell_id) REFERENCES cell_list(id)
);


DROP TABLE IF EXISTS message_by_cell;
CREATE TABLE message_by_cell(
	id SERIAL,
	cell_id BIGINT UNSIGNED NOT NULL,
	message_time TIMESTAMP DEFAULT NOW(),
	originate_number BIGINT UNSIGNED NOT NULL,
	terminate_number BIGINT UNSIGNED NOT NULL,
	message_len INT UNSIGNED NOT NULL,
	
	FOREIGN KEY (cell_id) REFERENCES cell_list(id)
);


DROP VIEW IF EXISTS v_bts_configuration;
CREATE VIEW v_bts_configuration as (
	SELECT
		reg.name as REG_NAME,	
		bts.name as BTS_NAME,
		COUNT(DISTINCT cell_number, cell_type_id) as CELL_CNT
	FROM bts_list bts
	INNER JOIN regions reg on reg.id = bts.region_id
	LEFT JOIN cell_list cell on bts.id = cell.bts_id
	WHERE (cell.bts_id) IN (
		SELECT bts_id
		FROM bts_cell_operations
		WHERE operation_type != 'dismantling')
	GROUP BY bts.name, reg.name 
	ORDER BY REG_NAME, BTS_NAME
);

DROP VIEW IF EXISTS v_stat_by_year;
CREATE VIEW v_stat_by_year as (
	SELECT
		reg.name as REG_NAME,	
		bts.name as BTS_NAME,
		COALESCE(DATE_FORMAT(s2.stime, '%Y'), DATE_FORMAT(s3.stime, '%Y'), DATE_FORMAT(s4.stime, '%Y'), DATE_FORMAT(so.stime, '%Y')) as S_YEAR,
		SUM(s2.sum_traf) as TRAF_2G,
		SUM(s3.sum_traf) as TRAF_3G,
		SUM(s4.sum_traf) as TRAF_4G,
		SUM(so.sum_traf) as TRAF_OTHER
	FROM bts_list bts
	INNER JOIN regions reg on reg.id = bts.region_id
	LEFT JOIN cell_list cell on bts.id = cell.bts_id
	LEFT JOIN stat_2g s2 ON s2.cell_id = cell.id
	LEFT JOIN stat_3g s3 ON s3.cell_id = cell.id
	LEFT JOIN stat_4g s4 ON s4.cell_id = cell.id
	LEFT JOIN stat_other so ON so.cell_id = cell.id
	GROUP BY bts.name, reg.name, COALESCE(DATE_FORMAT(s2.stime, '%Y'), DATE_FORMAT(s3.stime, '%Y'), DATE_FORMAT(s4.stime, '%Y'), DATE_FORMAT(so.stime, '%Y'))
	ORDER BY REG_NAME, BTS_NAME, S_YEAR
);

DELIMITER !!

DROP PROCEDURE IF EXISTS add_bts!!
CREATE PROCEDURE add_bts(IN bts_region INT, IN bts_num INT)
BEGIN
	DECLARE bts_cnt INT;
	SET bts_cnt = (
		SELECT COUNT(*)
		FROM bts_list
		WHERE region_id = bts_region AND bts_num = bts_number
	);
	IF bts_cnt = 0 THEN
		START TRANSACTION;
		INSERT INTO bts_list(region_id, bts_number, status) VALUES (bts_region, bts_num, 'f');
		COMMIT;
	END IF;
END!!

DROP PROCEDURE IF EXISTS cell_operation!!
CREATE PROCEDURE cell_operation(IN region_num INT, IN bts_if_add INT, IN cell_type VARCHAR(50), IN cell INT, IN act TEXT, IN designer VARCHAR(200), IN comment TEXT)
BEGIN
	DECLARE cell_cnt INT;
	DECLARE design_id INT;
	DECLARE type_id INT;
	DECLARE bts__id INT;
	DECLARE cell__id INT;
	DECLARE last_cell_state VARCHAR(30);
	SET cell_cnt = (
		SELECT COUNT(*)
		FROM cell_list c
		INNER JOIN cell_types t ON c.cell_type_id = t.id
		WHERE c.cell_number = cell AND t.name = cell_type AND c.bts_id = bts_if_add
	);
	SET design_id = (
		SELECT id
		FROM cell_designer
		WHERE first_last_name = designer
	);
	SET type_id = (
		SELECT id
		FROM cell_types
		WHERE name = cell_type
	);
	SET bts__id = (
		SELECT id
		FROM bts_list
		WHERE region_id = region_num AND bts_number = bts_if_add
	);
	SET cell__id = (
		SELECT id
		FROM cell_list
		WHERE cell_number = cell AND bts_id = bts__id AND cell_type_id = type_id
	);
	SET last_cell_state = (
		SELECT operation_type
		FROM bts_cell_operations
		WHERE cell_number = cell AND designer_id = design_id AND bts_id = bts__id AND cell_type_id = type_id
		ORDER BY operacion_date DESC
		LIMIT 1
	);
	IF act = 'add' THEN
		CALL add_bts(region_num, bts_if_add);
		IF cell_cnt = 0 THEN
			SET bts__id = (
				SELECT id
				FROM bts_list
				WHERE region_id = region_num AND bts_number = bts_if_add
			);
			START TRANSACTION;
			INSERT INTO cell_list(cell_number, cell_type_id, designer_id, bts_id, status) VALUES(cell, type_id, design_id, bts__id, 't');
			UPDATE bts_list
				SET status = 't'
				WHERE bts_number = bts__id;
			INSERT INTO bts_cell_operations(cell_id, operation_type, comment) VALUES (last_insert_id(), 'commissioning', comment);
			COMMIT;
		END IF;
	ELSEIF act = 'moderate' THEN
		IF cell_cnt = 1 AND last_cell_state != 'dismantling' THEN
			START TRANSACTION;
			UPDATE cell_list
				SET designer_id = design_id
				WHERE cell_number = cell AND designer_id = design_id AND bts_id = bts__id AND cell_type_id = type_id;
			INSERT INTO bts_cell_operations(cell_id, operation_type, comment) VALUES(cell__id, 'modification', comment);
			COMMIT;
		END IF;
		
	ELSEIF act = 'swap' THEN
		IF cell_cnt = 1 AND last_cell_state != 'dismantling' THEN
			START TRANSACTION;
			UPDATE cell_list
				SET designer_id = design_id
				WHERE cell_number = cell AND designer_id = design_id AND bts_id = bts__id AND cell_type_id = type_id;
			INSERT INTO bts_cell_operations(cell_id, operation_type, comment) VALUES(cell__id, 'dismantling', CONCAT('SWAP ', comment));
			COMMIT;
		END IF;
	ELSEIF act = 'dismantle' THEN
		IF cell_cnt = 1 AND last_cell_state != 'dismantling' THEN
			START TRANSACTION;
			UPDATE cell_list
				SET designer_id = design_id AND status = 'f'
				WHERE cell_number = cell AND designer_id = design_id AND bts_id = bts__id AND cell_type_id = type_id;
			INSERT INTO bts_cell_operations(cell_id, operation_type, comment) VALUES(cell__id, 'modification',  comment);
			COMMIT;
		END IF;
	ELSE
		SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Supported actions: add, moderate, swap, dismantle';
	END IF;
END!!

DROP TRIGGER IF EXISTS stat_2g_filling!!
CREATE TRIGGER stat_2g_filling BEFORE INSERT ON stat_2g
FOR EACH ROW
BEGIN
	SET NEW.stime = DATE_FORMAT(DATE_ADD(NEW.stime, INTERVAL 30 MINUTE),'%Y-%m-%d %H:00:00');
END!!

DROP TRIGGER IF EXISTS stat_3g_filling!!
CREATE TRIGGER stat_3g_filling BEFORE INSERT ON stat_3g
FOR EACH ROW
BEGIN
	SET NEW.stime = DATE_FORMAT(DATE_ADD(NEW.stime, INTERVAL 30 MINUTE),'%Y-%m-%d %H:00:00');
END!!

DROP TRIGGER IF EXISTS stat_4g_filling!!
CREATE TRIGGER stat_4g_filling BEFORE INSERT ON stat_4g
FOR EACH ROW
BEGIN
	SET NEW.stime = DATE_FORMAT(DATE_ADD(NEW.stime, INTERVAL 30 MINUTE),'%Y-%m-%d %H:00:00');
END!!

DROP TRIGGER IF EXISTS stat_other_filling!!
CREATE TRIGGER stat_other_filling BEFORE INSERT ON stat_other
FOR EACH ROW
BEGIN
	SET NEW.stime = DATE_FORMAT(DATE_ADD(NEW.stime, INTERVAL 30 MINUTE),'%Y-%m-%d %H:00:00');
END!!

DELIMITER ;
