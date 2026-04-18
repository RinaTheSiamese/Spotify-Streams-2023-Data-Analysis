-- =========================================================================
-- PROJECT TITLE: 
-- GITHUB: https://github.com/RinaTheSiamese
-- DATE: April 17, 2026
--
-- DESCRIPTION: 
-- 
--
-- The final output of these queries is designed to be exported and used 
-- to build a visualization dashboard in PowerBI and a presentation file.
-- =========================================================================

CREATE DATABASE spotify_streams_2023;
USE spotify_streams_2023;

-- ===============================================================================
-- SECTION 1: Bronze Layer
-- Desc: 	  Data is loaded into MySQL as-is. Nothing is changed, besides the
-- 			  addition of an ingestion column that notes the exact timestamp of
-- 			  when the data entered the pipeline.
-- ===============================================================================

-- 1.1 Create the Bronze table
CREATE TABLE bronze_spotify_data (
    track_name VARCHAR(255),
    `artist(s)_name` VARCHAR(255),
    artist_count VARCHAR(50),
    released_year INT,
    released_month INT,
    released_day INT,
    in_spotify_playlists INT,
    in_spotify_charts INT,
    streams VARCHAR(255),
    in_apple_playlists INT,
    in_apple_charts INT,
    in_deezer_playlists VARCHAR(50),
    in_deezer_charts INT,
    in_shazam_charts VARCHAR(50),
    bpm INT,
    `key` VARCHAR(50),
    `mode` VARCHAR(50),
    `danceability_%` INT,
    `valence_%` INT,
    `energy_%` INT,
    `acousticness_%` INT,
    `instrumentalness_%` INT,
    `liveness_%` INT,
    `speechiness_%` INT,
    ingested_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- 1.2 Load the Spotify Data into Bronze table
-- First, ensure local infile is enabled for this session
SET GLOBAL local_infile = 1;

LOAD DATA LOCAL INFILE 'C:/PERSONAL PROJECTS/spotify most streamed 2023 dataset/spotify-2023.csv'
INTO TABLE bronze_spotify_data
CHARACTER SET latin1
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
    track_name,
    `artist(s)_name`,
    artist_count,
    released_year,
    released_month,
    released_day,
    in_spotify_playlists,
    in_spotify_charts,
    streams,
    in_apple_playlists,
    in_apple_charts,
    in_deezer_playlists,
    in_deezer_charts,
    in_shazam_charts,
    bpm,
    `key`,
    `mode`,
    `danceability_%`,
    `valence_%`,
    `energy_%`,
    `acousticness_%`,
    `instrumentalness_%`,
    `liveness_%`,
    `speechiness_%`
);

-- ===============================================================================
-- SECTION 2: Silver Layer
-- Desc: 	  Proper data types shall be assigned to each attribute,
-- ===============================================================================

-- 2.1 Create the Silver table
CREATE TABLE silver_spotify_data (
    silver_id INT AUTO_INCREMENT PRIMARY KEY,
    track_name VARCHAR(255),
	artists_name VARCHAR(255),			-- we will split this into primary and secondary artist
--  primary_artist VARCHAR(255),
--  featured_artists VARCHAR(255),
    artist_count INT,
--  released_year VARCHAR(50),			-- we combined these to become a singular release date
--  released_month VARCHAR(50),
--  released_day VARCHAR(50),
    released_date DATE,
    in_spotify_playlists INT,
    in_spotify_charts INT,
    streams BIGINT,
    in_apple_playlists INT,
    in_apple_charts INT,
    in_deezer_playlists INT,
    in_deezer_charts INT,
    in_shazam_charts INT,
    bpm INT,
    music_key VARCHAR(255),				-- changed to music_key because 'key' alone is a SQL-reserved word
    music_mode VARCHAR(255),				-- changed to music_mode because 'mode' alone is a SQL-reserved word
    danceability_percent INT,			-- changed '%' symbol to the word 'percent'
    valence_percent INT,
    energy_percent INT,
    acousticness_percent INT,
    instrumentalness_percent INT,
    liveness_percent INT,
    speechiness_percent INT,
    cleaned_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE silver_spotify_data;

-- 2.2 Clean the data
-- 2.2.1 Handle missing values -------------------------------------------
-- A. Convert 'key' empty values to 'Unknown'
SELECT
    CASE
		WHEN `key` = '' THEN 'Unknown'
	END AS `key`
FROM bronze_spotify_data
WHERE `key` = '';

-- B. Convert 'in_shazam_charts' empty values to 0
SELECT
    CASE
		WHEN in_shazam_charts = '' THEN 0
	END AS in_shazam_charts
FROM bronze_spotify_data
WHERE in_shazam_charts = '';

-- 2.2.2 Handle logical duplicates ---------------------------------------
-- Combine duplicate records, given that they have exactly the same 'bpm',
-- 'key', 'mode', '%'s. As for those which do not meet the requirments, keeping only the most streamed.


-- 2.2.3 Handle corrupted data -------------------------------------------
-- A-1. Replace mojibake characters with their appropriate characters &
-- 	  Restore truncated texts (track_name)
SELECT
	track_name,
	CASE
		-- English Track Fixes
		WHEN track_name = 'I Can See You (Taylorï¿½ï¿½ï¿½s Version) (From The ' THEN 'I Can See You (Taylor''s Version) (From The Vault)'
		WHEN track_name = 'You Belong With Me (Taylorï¿½ï¿½ï¿½s Ve' THEN 'You Belong With Me (Taylor''s Version)'
		WHEN track_name = 'This Love (Taylorï¿½ï¿½ï¿½s Ve' THEN 'This Love (Taylor''s Version)'
		WHEN track_name = 'Donï¿½ï¿½ï¿½t Bl' THEN 'Don''t Blame Me'
		WHEN track_name = 'Devil Donï¿½ï¿½ï¿½' THEN 'Devil Don''t Know'
		WHEN track_name = 'Ainï¿½ï¿½ï¿½t Tha' THEN 'Ain''t That Some'
		WHEN track_name = 'Thinkinï¿½ï¿½ï¿½ B' THEN 'Thinkin'' Bout Me'
		WHEN track_name = 'Donï¿½ï¿½ï¿½t Break My' THEN 'Don''t Break My Heart'
		WHEN track_name = 'Evergreen (You Didnï¿½ï¿½ï¿½t Deserve Me A' THEN 'Evergreen (You Didn''t Deserve Me At All)'
		WHEN track_name = 'Here We Goï¿½ï¿½ï¿½ Again (feat. Tyler, the Cr' THEN 'Here We Go... Again (feat. Tyler, the Creator)'
		WHEN track_name = 'When Iï¿½ï¿½ï¿½m Gone (with Katy ' THEN 'When I''m Gone (with Katy Perry)'
		WHEN track_name = 'Cupid ï¿½ï¿½ï¿½ Twin Ver. (FIFTY FIFTY) ï¿½ï¿½ï¿½ Spe' THEN 'Cupid - Twin Ver. (FIFTY FIFTY) - Sped Up Version'
		WHEN track_name = 'Monï¿½ï¿½y so' THEN 'Monëy so big'

		-- Spanish Track Fixes
		WHEN track_name = 'Frï¿½ï¿½gil (feat. Grupo Front' THEN 'Frágil (feat. Grupo Frontera)'
		WHEN track_name = 'Tï¿½ï¿' THEN 'TQG'
		WHEN track_name = 'CORAZï¿½ï¿½N VA' THEN 'CORAZÓN VACÍO'
		WHEN track_name = 'Feliz Cumpleaï¿½ï¿½os Fe' THEN 'Feliz Cumpleaños Ferxxo'
		WHEN track_name = 'Acrï¿½ï¿½s' THEN 'Acróstico'
		WHEN track_name = 'Niï¿½ï¿½a Bo' THEN 'Niña Bonita'
		WHEN track_name = 'Arcï¿½ï¿½ngel: Bzrp Music Sessions, Vol' THEN 'Arcángel: Bzrp Music Sessions, Vol. 54'
		WHEN track_name = 'PLAYA DEL INGLï¿½' THEN 'PLAYA DEL INGLÉS'
		WHEN track_name = 'Monotonï¿½' THEN 'Monotonía'
		WHEN track_name = 'LA CANCIï¿½' THEN 'LA CANCIÓN'
		WHEN track_name = 'Quï¿½ï¿½ Ago' THEN 'Qué Agonía'
		WHEN track_name = 'Muï¿½ï¿½' THEN 'Muñecas'
		WHEN track_name = 'GATï¿½ï¿½' THEN 'GATÚBELA'
		WHEN track_name = 'Quï¿½ï¿½ Mï¿½ï¿' THEN 'Qué Más Pues?'
		WHEN track_name = 'Miï¿½ï¿½n' THEN 'Miénteme'
		WHEN track_name = 'Cayï¿½ï¿½ La Noche (feat. Cruz Cafunï¿½ï¿½, Abhir Hathi, Bejo, EL IMA)' THEN 'Cayó La Noche (feat. Cruz Cafuné, Abhir Hathi, Bejo, EL IMA)'
		WHEN track_name = 'Problemï¿½' THEN 'Problemón'
		WHEN track_name = 'Una Noche en Medellï¿½' THEN 'Una Noche en Medellín'
		WHEN track_name = 'X ï¿½ï¿½LTIMA' THEN 'X ÚLTIMA VEZ'
		WHEN track_name = 'RUMBATï¿½' THEN 'RUMBATÓN'
		WHEN track_name = 'Despuï¿½ï¿½s de la P' THEN 'Después de la Playa'
		WHEN track_name = 'Ensï¿½ï¿½ï¿½ï¿½ame ' THEN 'Enséñame a Bailar'
		WHEN track_name = 'El Apagï¿½' THEN 'El Apagón'
		WHEN track_name = 'TUS Lï¿½ï¿½GR' THEN 'TUS LÁGRIMAS'
		WHEN track_name = 'La Llevo Al Cielo (Ft. ï¿½ï¿½engo F' THEN 'La Llevo Al Cielo (Ft. Ñengo Flow)'
		WHEN track_name = 'cï¿½ï¿½mo dormi' THEN 'cómo dormiste?'
		WHEN track_name = 'Sin Seï¿½ï' THEN 'Sin Señal'
		WHEN track_name = 'Nostï¿½ï¿½l' THEN 'Nostálgico'
		WHEN track_name = 'Lï¿½ï¿½ï¿' THEN 'Lágrimas'

		-- Portuguese Track Fixes
		WHEN track_name = 'Novidade na ï¿½ï¿' THEN 'Novidade na Área'
		WHEN track_name = 'Novo Balanï¿½' THEN 'Novo Balanço'
		WHEN track_name = 'Cartï¿½ï¿½o B' THEN 'Cartão Black'
		WHEN track_name = 'Conexï¿½ï¿½es de Mï¿½ï¿½fia (feat. Rich ' THEN 'Conexões de Máfia (feat. Rich The Kid)'
		WHEN track_name = 'Leï¿½' THEN 'Leão'
		WHEN track_name = 'Sem Alianï¿½ï¿½a no ' THEN 'Sem Aliança no Dedo'
		WHEN track_name = 'Agudo Mï¿½ï¿½gi' THEN 'Agudo Mágico 3'
		WHEN track_name = 'Tubarï¿½ï¿½o Te' THEN 'Tubarão Te Amo'
		WHEN track_name = 'Malvadï¿½ï¿' THEN 'Malvadão 3'
		WHEN track_name = 'Vai Lï¿½ï¿½ Em Casa ' THEN 'Vai Lá Em Casa Hoje'
		WHEN track_name = 'Esqueï¿½ï¿½a-Me Se For C' THEN 'Esqueça-Me Se For Capaz'
		WHEN track_name = 'DANï¿½ï¿½A' THEN 'DANÇARINA'
		WHEN track_name = 'Seï¿½ï¿½o' THEN 'Sentadão'
		WHEN track_name = 'Cï¿½ï¿½' THEN 'Coração'
		WHEN track_name = 'Sï¿½ï¿½' THEN 'Sólo'

		-- Turkish / Other
		WHEN track_name = 'Piï¿½ï¿½man Deï¿' THEN 'Pişman Değilim'

		-- THE CATCH-ALL: For any remaining corrupted apostrophes we might have missed
		ELSE REPLACE(track_name, 'ï¿½ï¿½ï¿½', '''')
	END AS clean_track_name
FROM bronze_spotify_data
WHERE track_name LIKE '%ï¿½%';


-- A-2. Fix 'ýýý' in some titles
-- AI was utilized to figure out what these titles are based off other data within the record
SELECT
	track_name,
	CASE
		-- Prefix Errors (Hidden BOM characters)
		WHEN track_name = 'ýýýabcdefu' THEN 'abcdefu'
		WHEN track_name = 'ýýý98 Braves' THEN '98 Braves'

		-- Japanese Track Fixes (Triangulated using the Artist Name)
		WHEN track_name LIKE '%ýýý%' AND `artist(s)_name` LIKE 'YOASOBI' THEN 'Idol'
		WHEN track_name LIKE '%ýýý%' AND `artist(s)_name` LIKE 'Fujii Kaze' THEN 'Shinunoga E-Wa'
	ELSE track_name
    END AS fixed_track_name
FROM bronze_spotify_data;


-- B. Replace mojibake characters with their appropriate characters &
-- 	  Restore truncated texts (artist(s)_name)
SELECT
	`artist(s)_name`,
	CASE 
		-- Spanish & Latin Artists
		WHEN `artist(s)_name` = 'Rauw Alejandro, ROSALï¿½' THEN 'Rauw Alejandro, ROSALÍA'
		WHEN `artist(s)_name` = 'ROSALï¿½' THEN 'ROSALÍA'
		WHEN `artist(s)_name` = 'The Weeknd, ROSALï¿½' THEN 'The Weeknd, ROSALÍA'
		WHEN `artist(s)_name` = 'Wisin & Yandel, ROSALï¿½' THEN 'Wisin & Yandel, ROSALÍA'
		WHEN `artist(s)_name` = 'Jasiel Nuï¿½ï¿½ez, Peso P' THEN 'Jasiel Nuñez, Peso Pluma'
		WHEN `artist(s)_name` = 'Sebastian Yatra, Manuel Turizo, Beï¿½ï' THEN 'Sebastian Yatra, Manuel Turizo, Beéle'
		WHEN `artist(s)_name` = 'Bomba Estï¿½ï¿½reo, Bad B' THEN 'Bomba Estéreo, Bad Bunny'
		WHEN `artist(s)_name` = 'Junior H, Eden Muï¿½ï' THEN 'Junior H, Eden Muñoz'
		WHEN `artist(s)_name` = 'Eden Muï¿½ï' THEN 'Eden Muñoz'
		WHEN `artist(s)_name` = 'Justin Quiles, Lenny Tavï¿½ï¿½rez, BL' THEN 'Justin Quiles, Lenny Tavárez, BLVK JVCK'
		WHEN `artist(s)_name` = 'Arcangel, De La Ghetto, Justin Quiles, Lenny Tavï¿½ï¿½rez, Sech, Dalex, Dimelo Flow, Rich Music' THEN 'Arcangel, De La Ghetto, Justin Quiles, Lenny Tavárez, Sech, Dalex, Dimelo Flow, Rich Music'
		WHEN `artist(s)_name` = 'Quevedo, La Pantera, Juseph, Cruz Cafunï¿½ï¿½, Bï¿½ï¿½jo, Abhir Hathi' THEN 'Quevedo, La Pantera, Juseph, Cruz Cafuné, Bejo, Abhir Hathi'
		WHEN `artist(s)_name` = 'Josï¿½ï¿½ Felic' THEN 'José Feliciano'
		WHEN `artist(s)_name` = 'Bad Bunny, The Marï¿½ï' THEN 'Bad Bunny, The Marías'

		-- Brazilian & Portuguese Artists
		WHEN `artist(s)_name` = 'Zï¿½ï¿½ Neto & Crist' THEN 'Zé Neto & Cristiano'
		WHEN `artist(s)_name` = 'Zï¿½ï¿½ Fe' THEN 'Zé Felipe'
		WHEN `artist(s)_name` = 'Marï¿½ï¿½lia Mendo' THEN 'Marília Mendonça'
		WHEN `artist(s)_name` = 'Marï¿½ï¿½lia Mendonï¿½ï¿½a, George Henrique &' THEN 'Marília Mendonça, George Henrique & Rodrigo'
		WHEN `artist(s)_name` = 'Marï¿½ï¿½lia Mendonï¿½ï¿½a, Maiara &' THEN 'Marília Mendonça, Maiara & Maraisa'
		WHEN `artist(s)_name` = 'Marï¿½ï¿½lia Mendonï¿½ï¿½a, Hugo & G' THEN 'Marília Mendonça, Hugo & Guilherme'
		WHEN `artist(s)_name` = 'Dj LK da Escï¿½ï¿½cia, Tchakabum, mc jhenny, M' THEN 'Dj LK da Escócia, Tchakabum, mc jhenny, MC Ryan SP'
		WHEN `artist(s)_name` = 'Xamï¿½ï¿½, Gustah, Neo B' THEN 'Xamã, Gustah, Neo Beats'
		WHEN `artist(s)_name` = 'Matuï¿½ï¿½, Wiu, ' THEN 'Matuê, Wiu, Teto'
		WHEN `artist(s)_name` = 'Luï¿½ï¿½sa Sonza, MC Frog, Dj Gabriel do Borel, Davi K' THEN 'Luísa Sonza, MC Frog, Dj Gabriel do Borel, Davi Kneip'

		-- International / Electronic / Pop Artists
		WHEN `artist(s)_name` = 'Rï¿½ï¿½ma, Selena G' THEN 'Rema, Selena Gomez'
		WHEN `artist(s)_name` = 'Rï¿½ï' THEN 'Rema'
		WHEN `artist(s)_name` = 'Tiï¿½ï¿½sto, Tate M' THEN 'Tiësto, Tate McRae'
		WHEN `artist(s)_name` = 'Tiï¿½ï¿½sto, Ava' THEN 'Tiësto, Ava Max'
		WHEN `artist(s)_name` = 'Tiï¿½ï¿½sto, Kar' THEN 'Tiësto, KAROL G'
		WHEN `artist(s)_name` = 'Tiï¿½ï¿' THEN 'Tiësto'
		WHEN `artist(s)_name` = 'Mï¿½ï¿½ne' THEN 'Måneskin'
		WHEN `artist(s)_name` = 'Semicenk, Doï¿½ï¿½u ' THEN 'Semicenk, Doğu Swag'
		WHEN `artist(s)_name` = 'Luciano, Aitch, Bï¿½' THEN 'Luciano, Aitch, BIA'
		WHEN `artist(s)_name` = 'Schï¿½ï¿½rze, DJ R' THEN 'Schürze, DJ Robin'

		-- Movie Soundtracks
		WHEN `artist(s)_name` = 'Jordan Fisher, Josh Levi, Finneas O''Connell, 4*TOWN (From Disney and Pixarï¿½ï¿½ï¿½s Turning Red), Topher Ngo, Grayson Vill' THEN 'Jordan Fisher, Josh Levi, Finneas O''Connell, 4*TOWN (From Disney and Pixar''s Turning Red), Topher Ngo, Grayson Villanueva'

		-- The Catch-All (For anything minor we missed)
		ELSE REPLACE(`artist(s)_name`, 'ï¿½ï¿½ï¿½', '''')
	END AS artists_name
FROM bronze_spotify_data
WHERE `artist(s)_name` LIKE '%ï¿½%';

-- 2.2.4 Handle incorrect data types -------------------------------------
-- Remove the commas (,) in 'in_deezer_charts' and 'in_shazam_charts'
SELECT
	REPLACE(in_deezer_charts, ',', '') AS in_deezer_charts,
    REPLACE(in_shazam_charts, ',', '') AS in_shazam_charts
FROM bronze_spotify_data;

-- combine shazam charts = 0 and remove comma
SELECT
REPLACE(CASE
		WHEN in_shazam_charts = '' THEN 0
	ELSE in_shazam_charts
	END, ',', '') AS in_shazam_charts
FROM bronze_spotify_data;

-- 2.3 Load the data into the Silver table
INSERT INTO silver_spotify_data (
	track_name,
    artists_name,
    artist_count,
    released_date,
    in_spotify_playlists,
    in_spotify_charts,
    streams,
    in_apple_playlists,
    in_apple_charts,
    in_deezer_playlists,
    in_deezer_charts,
    in_shazam_charts,
    bpm,
    music_key,
    music_mode,
    danceability_percent,
    valence_percent,
    energy_percent,
    acousticness_percent,
    instrumentalness_percent,
    liveness_percent,
    speechiness_percent,
    cleaned_at)
SELECT
	-- Fix mojibake symbols and truncated 'track_name's
	CASE
		-- English Track Fixes
		WHEN track_name = 'I Can See You (Taylorï¿½ï¿½ï¿½s Version) (From The ' THEN 'I Can See You (Taylor''s Version) (From The Vault)'
		WHEN track_name = 'You Belong With Me (Taylorï¿½ï¿½ï¿½s Ve' THEN 'You Belong With Me (Taylor''s Version)'
		WHEN track_name = 'This Love (Taylorï¿½ï¿½ï¿½s Ve' THEN 'This Love (Taylor''s Version)'
		WHEN track_name = 'Donï¿½ï¿½ï¿½t Bl' THEN 'Don''t Blame Me'
		WHEN track_name = 'Devil Donï¿½ï¿½ï¿½' THEN 'Devil Don''t Know'
		WHEN track_name = 'Ainï¿½ï¿½ï¿½t Tha' THEN 'Ain''t That Some'
		WHEN track_name = 'Thinkinï¿½ï¿½ï¿½ B' THEN 'Thinkin'' Bout Me'
		WHEN track_name = 'Donï¿½ï¿½ï¿½t Break My' THEN 'Don''t Break My Heart'
		WHEN track_name = 'Evergreen (You Didnï¿½ï¿½ï¿½t Deserve Me A' THEN 'Evergreen (You Didn''t Deserve Me At All)'
		WHEN track_name = 'Here We Goï¿½ï¿½ï¿½ Again (feat. Tyler, the Cr' THEN 'Here We Go... Again (feat. Tyler, the Creator)'
		WHEN track_name = 'When Iï¿½ï¿½ï¿½m Gone (with Katy ' THEN 'When I''m Gone (with Katy Perry)'
		WHEN track_name = 'Cupid ï¿½ï¿½ï¿½ Twin Ver. (FIFTY FIFTY) ï¿½ï¿½ï¿½ Spe' THEN 'Cupid - Twin Ver. (FIFTY FIFTY) - Sped Up Version'
		WHEN track_name = 'Monï¿½ï¿½y so' THEN 'Monëy so big'

		-- Spanish Track Fixes
		WHEN track_name = 'Frï¿½ï¿½gil (feat. Grupo Front' THEN 'Frágil (feat. Grupo Frontera)'
		WHEN track_name = 'Tï¿½ï¿' THEN 'TQG'
		WHEN track_name = 'CORAZï¿½ï¿½N VA' THEN 'CORAZÓN VACÍO'
		WHEN track_name = 'Feliz Cumpleaï¿½ï¿½os Fe' THEN 'Feliz Cumpleaños Ferxxo'
		WHEN track_name = 'Acrï¿½ï¿½s' THEN 'Acróstico'
		WHEN track_name = 'Niï¿½ï¿½a Bo' THEN 'Niña Bonita'
		WHEN track_name = 'Arcï¿½ï¿½ngel: Bzrp Music Sessions, Vol' THEN 'Arcángel: Bzrp Music Sessions, Vol. 54'
		WHEN track_name = 'PLAYA DEL INGLï¿½' THEN 'PLAYA DEL INGLÉS'
		WHEN track_name = 'Monotonï¿½' THEN 'Monotonía'
		WHEN track_name = 'LA CANCIï¿½' THEN 'LA CANCIÓN'
		WHEN track_name = 'Quï¿½ï¿½ Ago' THEN 'Qué Agonía'
		WHEN track_name = 'Muï¿½ï¿½' THEN 'Muñecas'
		WHEN track_name = 'GATï¿½ï¿½' THEN 'GATÚBELA'
		WHEN track_name = 'Quï¿½ï¿½ Mï¿½ï¿' THEN 'Qué Más Pues?'
		WHEN track_name = 'Miï¿½ï¿½n' THEN 'Miénteme'
		WHEN track_name = 'Cayï¿½ï¿½ La Noche (feat. Cruz Cafunï¿½ï¿½, Abhir Hathi, Bejo, EL IMA)' THEN 'Cayó La Noche (feat. Cruz Cafuné, Abhir Hathi, Bejo, EL IMA)'
		WHEN track_name = 'Problemï¿½' THEN 'Problemón'
		WHEN track_name = 'Una Noche en Medellï¿½' THEN 'Una Noche en Medellín'
		WHEN track_name = 'X ï¿½ï¿½LTIMA' THEN 'X ÚLTIMA VEZ'
		WHEN track_name = 'RUMBATï¿½' THEN 'RUMBATÓN'
		WHEN track_name = 'Despuï¿½ï¿½s de la P' THEN 'Después de la Playa'
		WHEN track_name = 'Ensï¿½ï¿½ï¿½ï¿½ame ' THEN 'Enséñame a Bailar'
		WHEN track_name = 'El Apagï¿½' THEN 'El Apagón'
		WHEN track_name = 'TUS Lï¿½ï¿½GR' THEN 'TUS LÁGRIMAS'
		WHEN track_name = 'La Llevo Al Cielo (Ft. ï¿½ï¿½engo F' THEN 'La Llevo Al Cielo (Ft. Ñengo Flow)'
		WHEN track_name = 'cï¿½ï¿½mo dormi' THEN 'cómo dormiste?'
		WHEN track_name = 'Sin Seï¿½ï' THEN 'Sin Señal'
		WHEN track_name = 'Nostï¿½ï¿½l' THEN 'Nostálgico'
		WHEN track_name = 'Lï¿½ï¿½ï¿' THEN 'Lágrimas'

		-- Portuguese Track Fixes
		WHEN track_name = 'Novidade na ï¿½ï¿' THEN 'Novidade na Área'
		WHEN track_name = 'Novo Balanï¿½' THEN 'Novo Balanço'
		WHEN track_name = 'Cartï¿½ï¿½o B' THEN 'Cartão Black'
		WHEN track_name = 'Conexï¿½ï¿½es de Mï¿½ï¿½fia (feat. Rich ' THEN 'Conexões de Máfia (feat. Rich The Kid)'
		WHEN track_name = 'Leï¿½' THEN 'Leão'
		WHEN track_name = 'Sem Alianï¿½ï¿½a no ' THEN 'Sem Aliança no Dedo'
		WHEN track_name = 'Agudo Mï¿½ï¿½gi' THEN 'Agudo Mágico 3'
		WHEN track_name = 'Tubarï¿½ï¿½o Te' THEN 'Tubarão Te Amo'
		WHEN track_name = 'Malvadï¿½ï¿' THEN 'Malvadão 3'
		WHEN track_name = 'Vai Lï¿½ï¿½ Em Casa ' THEN 'Vai Lá Em Casa Hoje'
		WHEN track_name = 'Esqueï¿½ï¿½a-Me Se For C' THEN 'Esqueça-Me Se For Capaz'
		WHEN track_name = 'DANï¿½ï¿½A' THEN 'DANÇARINA'
		WHEN track_name = 'Seï¿½ï¿½o' THEN 'Sentadão'
		WHEN track_name = 'Cï¿½ï¿½' THEN 'Coração'
		WHEN track_name = 'Sï¿½ï¿½' THEN 'Sólo'

		-- Turkish / Other
		WHEN track_name = 'Piï¿½ï¿½man Deï¿' THEN 'Pişman Değilim'
        
		-- Prefix Errors (Hidden BOM characters)
		WHEN track_name = 'ýýýabcdefu' THEN 'abcdefu'
		WHEN track_name = 'ýýý98 Braves' THEN '98 Braves'

		-- Japanese Track Fixes (Triangulated using the Artist Name)
		WHEN track_name LIKE '%ýýý%' AND `artist(s)_name` LIKE 'YOASOBI' THEN 'Idol'
		WHEN track_name LIKE '%ýýý%' AND `artist(s)_name` LIKE 'Fujii Kaze' THEN 'Shinunoga E-Wa'

		-- THE CATCH-ALL: For any remaining corrupted apostrophes we might have missed
		ELSE REPLACE(track_name, 'ï¿½ï¿½ï¿½', '''')
	END AS track_name,
    -- Fix mojibake symbols and truncated 'artists_name's
    CASE 
		-- Spanish & Latin Artists
		WHEN `artist(s)_name` = 'Rauw Alejandro, ROSALï¿½' THEN 'Rauw Alejandro, ROSALÍA'
		WHEN `artist(s)_name` = 'ROSALï¿½' THEN 'ROSALÍA'
		WHEN `artist(s)_name` = 'The Weeknd, ROSALï¿½' THEN 'The Weeknd, ROSALÍA'
		WHEN `artist(s)_name` = 'Wisin & Yandel, ROSALï¿½' THEN 'Wisin & Yandel, ROSALÍA'
		WHEN `artist(s)_name` = 'Jasiel Nuï¿½ï¿½ez, Peso P' THEN 'Jasiel Nuñez, Peso Pluma'
		WHEN `artist(s)_name` = 'Sebastian Yatra, Manuel Turizo, Beï¿½ï' THEN 'Sebastian Yatra, Manuel Turizo, Beéle'
		WHEN `artist(s)_name` = 'Bomba Estï¿½ï¿½reo, Bad B' THEN 'Bomba Estéreo, Bad Bunny'
		WHEN `artist(s)_name` = 'Junior H, Eden Muï¿½ï' THEN 'Junior H, Eden Muñoz'
		WHEN `artist(s)_name` = 'Eden Muï¿½ï' THEN 'Eden Muñoz'
		WHEN `artist(s)_name` = 'Justin Quiles, Lenny Tavï¿½ï¿½rez, BL' THEN 'Justin Quiles, Lenny Tavárez, BLVK JVCK'
		WHEN `artist(s)_name` = 'Arcangel, De La Ghetto, Justin Quiles, Lenny Tavï¿½ï¿½rez, Sech, Dalex, Dimelo Flow, Rich Music' THEN 'Arcangel, De La Ghetto, Justin Quiles, Lenny Tavárez, Sech, Dalex, Dimelo Flow, Rich Music'
		WHEN `artist(s)_name` = 'Quevedo, La Pantera, Juseph, Cruz Cafunï¿½ï¿½, Bï¿½ï¿½jo, Abhir Hathi' THEN 'Quevedo, La Pantera, Juseph, Cruz Cafuné, Bejo, Abhir Hathi'
		WHEN `artist(s)_name` = 'Josï¿½ï¿½ Felic' THEN 'José Feliciano'
		WHEN `artist(s)_name` = 'Bad Bunny, The Marï¿½ï' THEN 'Bad Bunny, The Marías'

		-- Brazilian & Portuguese Artists
		WHEN `artist(s)_name` = 'Zï¿½ï¿½ Neto & Crist' THEN 'Zé Neto & Cristiano'
		WHEN `artist(s)_name` = 'Zï¿½ï¿½ Fe' THEN 'Zé Felipe'
		WHEN `artist(s)_name` = 'Marï¿½ï¿½lia Mendo' THEN 'Marília Mendonça'
		WHEN `artist(s)_name` = 'Marï¿½ï¿½lia Mendonï¿½ï¿½a, George Henrique &' THEN 'Marília Mendonça, George Henrique & Rodrigo'
		WHEN `artist(s)_name` = 'Marï¿½ï¿½lia Mendonï¿½ï¿½a, Maiara &' THEN 'Marília Mendonça, Maiara & Maraisa'
		WHEN `artist(s)_name` = 'Marï¿½ï¿½lia Mendonï¿½ï¿½a, Hugo & G' THEN 'Marília Mendonça, Hugo & Guilherme'
		WHEN `artist(s)_name` = 'Dj LK da Escï¿½ï¿½cia, Tchakabum, mc jhenny, M' THEN 'Dj LK da Escócia, Tchakabum, mc jhenny, MC Ryan SP'
		WHEN `artist(s)_name` = 'Xamï¿½ï¿½, Gustah, Neo B' THEN 'Xamã, Gustah, Neo Beats'
		WHEN `artist(s)_name` = 'Matuï¿½ï¿½, Wiu, ' THEN 'Matuê, Wiu, Teto'
		WHEN `artist(s)_name` = 'Luï¿½ï¿½sa Sonza, MC Frog, Dj Gabriel do Borel, Davi K' THEN 'Luísa Sonza, MC Frog, Dj Gabriel do Borel, Davi Kneip'

		-- International / Electronic / Pop Artists
		WHEN `artist(s)_name` = 'Rï¿½ï¿½ma, Selena G' THEN 'Rema, Selena Gomez'
		WHEN `artist(s)_name` = 'Rï¿½ï' THEN 'Rema'
		WHEN `artist(s)_name` = 'Tiï¿½ï¿½sto, Tate M' THEN 'Tiësto, Tate McRae'
		WHEN `artist(s)_name` = 'Tiï¿½ï¿½sto, Ava' THEN 'Tiësto, Ava Max'
		WHEN `artist(s)_name` = 'Tiï¿½ï¿½sto, Kar' THEN 'Tiësto, KAROL G'
		WHEN `artist(s)_name` = 'Tiï¿½ï¿' THEN 'Tiësto'
		WHEN `artist(s)_name` = 'Mï¿½ï¿½ne' THEN 'Måneskin'
		WHEN `artist(s)_name` = 'Semicenk, Doï¿½ï¿½u ' THEN 'Semicenk, Doğu Swag'
		WHEN `artist(s)_name` = 'Luciano, Aitch, Bï¿½' THEN 'Luciano, Aitch, BIA'
		WHEN `artist(s)_name` = 'Schï¿½ï¿½rze, DJ R' THEN 'Schürze, DJ Robin'

		-- Movie Soundtracks
		WHEN `artist(s)_name` = 'Jordan Fisher, Josh Levi, Finneas O''Connell, 4*TOWN (From Disney and Pixarï¿½ï¿½ï¿½s Turning Red), Topher Ngo, Grayson Vill' THEN 'Jordan Fisher, Josh Levi, Finneas O''Connell, 4*TOWN (From Disney and Pixar''s Turning Red), Topher Ngo, Grayson Villanueva'

		-- The Catch-All (For anything minor we missed)
		ELSE REPLACE(`artist(s)_name`, 'ï¿½ï¿½ï¿½', '''')
	END AS artists_name,
    artist_count,
    -- Consolidate released_year, released_month, and released_day
	STR_TO_DATE(CONCAT(released_year, '-', released_month, '-', released_day), '%Y-%m-%d') AS released_date,
    in_spotify_playlists,
    in_spotify_charts,
    streams,
    in_apple_playlists,
    in_apple_charts,
    REPLACE(in_deezer_playlists, ',', '') AS in_deezer_playlists,
    REPLACE(in_deezer_charts, ',', '') AS in_deezer_charts,
    -- Convert 'in_shazam_charts' empty values to 0
    REPLACE(CASE
		WHEN in_shazam_charts = '' THEN 0
	ELSE in_shazam_charts
	END, ',', '') AS in_shazam_charts,
    bpm,
    -- Convert missing values in 'key' to 'unknown'
    CASE
		WHEN `key` = '' THEN 'Unknown'
	ELSE `key`
	END AS `key`,
    `mode`,
    `danceability_%`,
    `valence_%`,
    `energy_%`,
    `acousticness_%`,
    `instrumentalness_%`,
    `liveness_%`,
    `speechiness_%`,
    CURRENT_TIMESTAMP() as cleaned_at
FROM bronze_spotify_data
-- Filter/drop out the corrupted Edison Lighthouse row
WHERE streams != 'BPM110KeyAModeMajorDanceability53Valence75Energy69Acousticness7Instrumentalness0Liveness17Speechiness3';

DROP TABLE silver_spotify_data;



-- COPY PASTE CREATE
CREATE TABLE silver_spotify_data (
    silver_id INT AUTO_INCREMENT PRIMARY KEY,
    track_name VARCHAR(255),
--  artists_name VARCHAR(255),			-- we will split this into primary and secondary artist
    primary_artist VARCHAR(255),
    featured_artists VARCHAR(255),
    artist_count INT,
--  released_year VARCHAR(50),			-- we combined these to become a singular release date
--  released_month VARCHAR(50),
--  released_day VARCHAR(50),
    released_date DATE,
    in_spotify_playlists INT,
    in_spotify_charts INT,
    streams BIGINT,
    in_apple_playlists INT,
    in_apple_charts INT,
    in_deezer_playlists INT,
    in_deezer_charts INT,
    in_shazam_charts INT,
    bpm INT,
    music_key VARCHAR(2),				-- changed to music_key because 'key' alone is a SQL-reserved word
    music_mode VARCHAR(10),				-- changed to music_mode because 'mode' alone is a SQL-reserved word
    danceability_percent INT,			-- changed '%' symbol to the word 'percent'
    valence_percent INT,
    energy_percent INT,
    acousticness_percent INT,
    instrumentalness_percent INT,
    liveness_percent INT,
    speechiness_percent INT,
    cleaned_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
