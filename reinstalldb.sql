-- Must run this as the MYSQL admin user or you will be kicked off, too!

USE mysql;

SET GLOBAL offline_mode = ON;

DROP FUNCTION IF EXISTS lib_mysqludf_amqp_info;
CREATE FUNCTION lib_mysqludf_amqp_info RETURNS STRING SONAME 'lib_mysqludf_amqp.so';
DROP FUNCTION IF EXISTS lib_mysqludf_amqp_sendjson;
CREATE FUNCTION lib_mysqludf_amqp_sendjson RETURNS STRING SONAME 'lib_mysqludf_amqp.so';
DROP FUNCTION IF EXISTS lib_mysqludf_amqp_sendstring;
CREATE FUNCTION lib_mysqludf_amqp_sendstring RETURNS STRING SONAME 'lib_mysqludf_amqp.so';

SET GLOBAL offline_mode = OFF;

SELECT CONVERT(lib_mysqludf_amqp_info() USING UTF8);
