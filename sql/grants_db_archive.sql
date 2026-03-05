-- Minimal privileges for DB archive account.
-- Replace ARCHIVE_USER and strong password first.

CREATE USER IF NOT EXISTS 'ARCHIVE_USER'@'localhost' IDENTIFIED BY 'CHANGE_ME_STRONG_PASSWORD';

GRANT SELECT ON `epcenergy_ess_01`.* TO 'ARCHIVE_USER'@'localhost';

GRANT ALTER, CREATE, DROP
ON `epcenergy_ess_01`.`ess_string_HON`
TO 'ARCHIVE_USER'@'localhost';

GRANT ALTER, CREATE, DROP
ON `epcenergy_ess_01`.`ess_string_HONSJ`
TO 'ARCHIVE_USER'@'localhost';

GRANT ALTER, CREATE, DROP
ON `epcenergy_ess_01`.`ess_string_0000`
TO 'ARCHIVE_USER'@'localhost';

GRANT ALTER, CREATE, DROP
ON `epcenergy_ess_01`.`ess_string_DLN`
TO 'ARCHIVE_USER'@'localhost';

FLUSH PRIVILEGES;
