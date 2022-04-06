/**
 ** Demo source data from the dummy data of "data lake" demo
 **/
-- 
-- default database should be INT or INTEGRATION
--
USE DATABASE INT;

/********************************************************************
 ** Reference data create section
 ********************************************************************/
USE DATABASE INT;
CREATE SCHEMA IF NOT EXISTS REFERENCE;
MERGE INTO REFERENCE.TILE_SCORE D
USING (
    --CREATE OR REPLACE TABLE REFERENCE.TILE_SCORE AS
    SELECT $1::VARCHAR TITLE,
        $2::FLOAT SCORE
    FROM VALUES 
      ('C-level', 20),
      ('VP', 15),
      ('Director', 10),
      ('Manager', 5)
) S
ON D.TITLE = S.TITLE
WHEN NOT MATCHED THEN INSERT(TITLE, SCORE) VALUES(S.TITLE, S.SCORE)
WHEN MATCHED THEN UPDATE SET SCORE = S.SCORE;

MERGE INTO REFERENCE.PLATFORM D
USING (
    --CREATE OR REPLACE TABLE REFERENCE.PLATFORM AS
    SELECT $1::NUMBER PLATFORM_ID,
        $2::VARCHAR PLATFORM_NAME ,
        $3::VARCHAR PLATFORM_TYPE
    FROM VALUES 
      (1, 'IH_EMAIL_GOWS', 'GOWS'),
      (2, 'SFDC_EMAIL_MSTM', 'MSTM')
) S
ON D.PLATFORM_NAME = S.PLATFORM_NAME
AND D.PLATFORM_TYPE = S.PLATFORM_TYPE
WHEN NOT MATCHED THEN 
    INSERT(PLATFORM_ID, PLATFORM_NAME, PLATFORM_TYPE) 
    VALUES(S.PLATFORM_ID, S.PLATFORM_NAME, S.PLATFORM_TYPE)
WHEN MATCHED THEN 
    UPDATE SET 
        PLATFORM_ID = S.PLATFORM_ID,
        PLATFORM_NAME = S.PLATFORM_NAME,
        PLATFORM_TYPE = S.PLATFORM_TYPE;

MERGE INTO REFERENCE.COMPANY D
USING (
    --CREATE OR REPLACE TABLE REFERENCE.COMPANY AS
    SELECT $1::NUMBER COMPANY_ID,
        $2::VARCHAR COMPANY_NAME,
        $3::VARCHAR SHORT_NAME
    FROM VALUES 
      (1, 'INTROHIVE', 'IH'),
      (2, 'SALESFORCE', 'SFDC')
) S
ON D.COMPANY_NAME = S.COMPANY_NAME
WHEN NOT MATCHED THEN 
    INSERT(COMPANY_ID, COMPANY_NAME, SHORT_NAME) 
    VALUES(S.COMPANY_ID, S.COMPANY_NAME, S.SHORT_NAME)
WHEN MATCHED THEN 
    UPDATE SET 
        COMPANY_ID = S.COMPANY_ID,
        COMPANY_NAME = S.COMPANY_NAME,
        SHORT_NAME = S.SHORT_NAME;


/********************************************************************
 ** Schema Configuration Section
 ********************************************************************/
--
-- execute context
--
USE SCHEMA _METADATA;

-- Clear any existing test data
TRUNCATE TABLE CTRL_IMPORT;
TRUNCATE TABLE CTRL_CURRENT;
TRUNCATE TABLE CTRL_LOG;

--
-- Create some test config data
--
MERGE INTO CTRL_IMPORT D
USING (
    SELECT $1 CLIENT_NAME,
        $2 PLATFORM_NAME,
        $3 PLATFORM_TYPE,
        'INT' DATA_CATALOG,
        CONCAT($1, '_', $2, '_', $3) DATA_SCHEMA,
        $4 DATA_NAME,
        NULL /*PARSE_JSON($5)*/ DATA_STAGE,
        NULL /*PARSE_JSON($6)*/ DATA_FORMAT,
        PARSE_JSON('{"DATA_KEY_FIELD": "DATA_KEY", "DATA_HASH_FIELD": "DATA_HASH","DATA_TIME_FIELD": "LOAD_TIME"}') CTRL_FIELD,
        PARSE_JSON($7) DATA_FIELD,
        PARSE_JSON($8) META_FIELD,
        $9 TRANSFORMATION,
        MD5(TO_VARCHAR(ARRAY_CONSTRUCT(*))) CONFIG_HASH,
        TRUE AUTOMATED
    FROM VALUES 
      (
        'IH',
        'EMAIL',
        'GOWS',
        'PERSON',
        $$['URL = \'s3://af-data-des-sandbox/ingest/jing/data1/\'', 'STORAGE_INTEGRATION = DES_INTEGRATION']$$,
        $$['TYPE = JSON']$$,
        $$[
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "FIRST_NAME",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "LAST_NAME",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "TITLE",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "COMPANY",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "PHOTO_URL",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "LOAD_TIME",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TIMESTAMP_NTZ"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "PERSON_ID",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "NTRL_SRC_STM_KEY",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "DATA_KEY",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "DATA_HASH",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            }
        ]$$,
        $$[
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "FULL_NAME",
                "FIELD_TRANS": "CONCAT(FIRST_NAME, ' ', LAST_NAME)",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "SCORE",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "FLOAT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "COMPANY_ID",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "PLATFORM_ID",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "BUSINESS_RUN_DATE",
                "FIELD_TRANS": "CURRENT_DATE",
                "FIELD_TYPE": "DATE"
            }
        ]$$,
        $$
        SELECT P.*, S.SCORE, C.COMPANY_ID, F.PLATFORM_ID
        FROM STG.IH_EMAIL_GOWS.DIGEST_PERSON P 
        LEFT JOIN REFERENCE.TILE_SCORE S ON P.TITLE = S.TITLE
        LEFT JOIN REFERENCE.COMPANY C ON P.COMPANY = C.SHORT_NAME
        LEFT JOIN REFERENCE.PLATFORM F ON P.PLATFORM = F.PLATFORM_NAME
        $$
      ),
      (
        'IH',
        'EMAIL',
        'GOWS',
        'PERSON_EMAIL',
        $$['URL = \'s3://af-data-des-sandbox/ingest/jing/data1/\'', 'STORAGE_INTEGRATION = DES_INTEGRATION']$$,
        $$['TYPE = JSON']$$,
        $$[
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "PERSON_ID",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "EMAIL",
                "FIELD_TRANS": "PERSON_EMAIL",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "DISPLAY_NAME",
                "FIELD_TRANS": "PERSON_DISPLAY_NAME",
                "FIELD_TYPE": "VARIANT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "LOAD_TIME",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TIMESTAMP_NTZ"
            }
        ]$$,
        $$[
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "PERSON_EMAIL_ID",
                "FIELD_TRANS": "_METADATA.SG_KEY_GEN.NEXTVAL",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "NTRL_SRC_STM_KEY",
                "FIELD_TRANS": "CONCAT({{KEY_LIST}})",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "DATA_KEY",
                "FIELD_TRANS": "MD5(CONCAT({{KEY_LIST}}))",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "DATA_HASH",
                "FIELD_TRANS": "MD5(TO_VARCHAR(ARRAY_CONSTRUCT({{HASH_LIST}})))",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "PLATFORM_ID",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "BUSINESS_RUN_DATE",
                "FIELD_TRANS": "CURRENT_DATE",
                "FIELD_TYPE": "DATE"
            }
        ]$$,
        $$
        SELECT P.*, F.PLATFORM_ID,
            EMAIL_PARSER(VALUE)['email'] PERSON_EMAIL, 
            EMAIL_PARSER(VALUE)['name'] PERSON_DISPLAY_NAME
        FROM STG.IH_EMAIL_GOWS.DIGEST_PERSON P
        LEFT JOIN REFERENCE.PLATFORM F ON P.PLATFORM = F.PLATFORM_NAME,
        LATERAL FLATTEN (INPUT => EMAIL_ADDRESS)
        $$
      ),
      (
        'IH',
        'EMAIL',
        'GOWS',
        'MESSAGE',
        $$['URL = \'s3://af-data-des-sandbox/ingest/jing/data1/\'', 'STORAGE_INTEGRATION = DES_INTEGRATION']$$,
        $$['TYPE = JSON']$$,
        $$[
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "SUBJECT",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "SENDER",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "BODY",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "REFERENCES",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "VARIANT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "SENT_AT",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TIMESTAMP_NTZ"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "PROBABLY_REPLY",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "BOOLEAN"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "AUTO_RESPONSE_TYPE",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "IN_REPLY_TO",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "ORIGINAL_ID",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "THREAD_ID",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "LOAD_TIME",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TIMESTAMP_NTZ"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "MESSAGE_ID",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "NTRL_SRC_STM_KEY",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "DATA_KEY",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "DATA_HASH",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            }
        ]$$,
        $$[
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "PLATFORM_ID",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "BUSINESS_RUN_DATE",
                "FIELD_TRANS": "CURRENT_DATE",
                "FIELD_TYPE": "DATE"
            }
        ]$$,
        $$
        SELECT M.*, F.PLATFORM_ID
        FROM STG.IH_EMAIL_GOWS.DIGEST_MESSAGE M
        LEFT JOIN REFERENCE.PLATFORM F
        ON M.PLATFORM = F.PLATFORM_NAME
        $$
      ),
      (
        'IH',
        'EMAIL',
        'GOWS',
        'MESSAGE_EMAIL',
        $$['URL = \'s3://af-data-des-sandbox/ingest/jing/data1/\'', 'STORAGE_INTEGRATION = DES_INTEGRATION']$$,
        $$['TYPE = JSON']$$,
        $$[
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "MESSAGE_ID",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "SENDER",
                "FIELD_TRANS": "EMAIL_PARSER(SENDER)['email']",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "RECIPIENT_EMAIL_ADDRESS",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "RECIPIENT_DISPLAY_NAME",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "VARIANT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "RECIPIENT_TYPE",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "LOAD_TIME",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TIMESTAMP_NTZ"
            }
        ]$$,
        $$[
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "MESSAGE_EMAIL_ID",
                "FIELD_TRANS": "_METADATA.SG_KEY_GEN.NEXTVAL",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "NTRL_SRC_STM_KEY",
                "FIELD_TRANS": "CONCAT({{KEY_LIST}})",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "DATA_KEY",
                "FIELD_TRANS": "MD5(CONCAT({{KEY_LIST}}))",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "DATA_HASH",
                "FIELD_TRANS": "MD5(TO_VARCHAR(ARRAY_CONSTRUCT({{HASH_LIST}})))",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "PLATFORM_ID",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "BUSINESS_RUN_DATE",
                "FIELD_TRANS": "CURRENT_DATE",
                "FIELD_TYPE": "DATE"
            }
        ]$$,
        $$
        SELECT M.*, F.PLATFORM_ID,
            EMAIL_PARSER(VALUE)['email'] RECIPIENT_EMAIL_ADDRESS, 
            EMAIL_PARSER(VALUE)['name'] RECIPIENT_DISPLAY_NAME,
            'TO' RECIPIENT_TYPE
        FROM STG.IH_EMAIL_GOWS.DIGEST_MESSAGE M
        LEFT JOIN REFERENCE.PLATFORM F ON M.PLATFORM = F.PLATFORM_NAME,
        LATERAL FLATTEN (INPUT => RECIPIENTS)
        $$
      ),
      (
        'IH',
        'EMAIL',
        'GOWS',
        'EMAIL_ADDRESS',
        $$['URL = \'s3://af-data-des-sandbox/ingest/jing/data1/\'', 'STORAGE_INTEGRATION = DES_INTEGRATION']$$,
        $$['TYPE = JSON']$$,
        $$[
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "EMAIL_ADDRESS",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "DISPLAY_NAME",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "VARIANT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "LOAD_TIME",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TIMESTAMP_NTZ"
            }
        ]$$,
        $$[
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "EMAIL_ADDRESS_ID",
                "FIELD_TRANS": "_METADATA.SG_KEY_GEN.NEXTVAL",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "NTRL_SRC_STM_KEY",
                "FIELD_TRANS": "CONCAT({{KEY_LIST}})",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "DATA_KEY",
                "FIELD_TRANS": "MD5(CONCAT({{KEY_LIST}}))",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "DATA_HASH",
                "FIELD_TRANS": "MD5(TO_VARCHAR(ARRAY_CONSTRUCT({{HASH_LIST}})))",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "PLATFORM_ID",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "BUSINESS_RUN_DATE",
                "FIELD_TRANS": "CURRENT_DATE",
                "FIELD_TYPE": "DATE"
            }
        ]$$,
        $$
        SELECT EMAIL_ADDRESS, F.PLATFORM_ID,
            ARRAY_AGG(DISPLAY_NAME) DISPLAY_NAME,
            MIN(CREATE_AT) CREATE_AT,
            MAX(LOAD_TIME) LOAD_TIME
        FROM (
            SELECT PLATFORM,
                EMAIL_PARSER(VALUE)['email'] EMAIL_ADDRESS, 
                EMAIL_PARSER(VALUE)['name'] DISPLAY_NAME,
                MIN(LOAD_TIME) CREATE_AT,
                MAX(LOAD_TIME) LOAD_TIME
             FROM (
                SELECT EMAIL_ADDRESS, PLATFORM, LOAD_TIME 
                FROM STG.IH_EMAIL_GOWS.DIGEST_PERSON
                UNION ALL
                SELECT ARRAY_APPEND(RECIPIENTS, SENDER), PLATFORM, LOAD_TIME
                FROM STG.IH_EMAIL_GOWS.DIGEST_MESSAGE
            ), LATERAL FLATTEN (INPUT => EMAIL_ADDRESS)
            GROUP BY 1,2,3
        ) A
        LEFT JOIN REFERENCE.PLATFORM F ON A.PLATFORM = F.PLATFORM_NAME
        GROUP BY 1,2
        $$
      ),
      (
        'SFDC',
        'EMAIL',
        'MSTM',
        'PERSON',
        $$['URL = \'s3://af-data-des-sandbox/ingest/jing/data2/\'', 'STORAGE_INTEGRATION = DES_INTEGRATION']$$,
        $$['TYPE = JSON']$$,
        $$[
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "FIRST_NAME",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "LAST_NAME",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "TITLE",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "COMPANY",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "PHOTO_URL",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "LOAD_TIME",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TIMESTAMP_NTZ"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "PERSON_ID",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "NTRL_SRC_STM_KEY",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "DATA_KEY",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "DATA_HASH",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            }
        ]$$,
        $$[
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "FULL_NAME",
                "FIELD_TRANS": "CONCAT(FIRST_NAME, ' ', LAST_NAME)",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "SCORE",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "FLOAT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "COMPANY_ID",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "PLATFORM_ID",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "BUSINESS_RUN_DATE",
                "FIELD_TRANS": "CURRENT_DATE",
                "FIELD_TYPE": "DATE"
            }
        ]$$,
        $$
        SELECT P.*, S.SCORE, C.COMPANY_ID, F.PLATFORM_ID
        FROM STG.SFDC_EMAIL_MSTM.DIGEST_PERSON P 
        LEFT JOIN REFERENCE.TILE_SCORE S ON P.TITLE = S.TITLE
        LEFT JOIN REFERENCE.COMPANY C ON P.COMPANY = C.SHORT_NAME
        LEFT JOIN REFERENCE.PLATFORM F ON P.PLATFORM = F.PLATFORM_NAME
        $$
      ),
      (
        'SFDC',
        'EMAIL',
        'MSTM',
        'PERSON_EMAIL',
        $$['URL = \'s3://af-data-des-sandbox/ingest/jing/data1/\'', 'STORAGE_INTEGRATION = DES_INTEGRATION']$$,
        $$['TYPE = JSON']$$,
        $$[
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "PERSON_ID",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "EMAIL",
                "FIELD_TRANS": "PERSON_EMAIL",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "DISPLAY_NAME",
                "FIELD_TRANS": "PERSON_DISPLAY_NAME",
                "FIELD_TYPE": "VARIANT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "LOAD_TIME",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TIMESTAMP_NTZ"
            }
        ]$$,
        $$[
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "PERSION_EMAIL_ID",
                "FIELD_TRANS": "_METADATA.SG_KEY_GEN.NEXTVAL",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "NTRL_SRC_STM_KEY",
                "FIELD_TRANS": "CONCAT({{KEY_LIST}})",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "DATA_KEY",
                "FIELD_TRANS": "MD5(CONCAT({{KEY_LIST}}))",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "DATA_HASH",
                "FIELD_TRANS": "MD5(TO_VARCHAR(ARRAY_CONSTRUCT({{HASH_LIST}})))",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "PLATFORM_ID",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "BUSINESS_RUN_DATE",
                "FIELD_TRANS": "CURRENT_DATE",
                "FIELD_TYPE": "DATE"
            }
        ]$$,
        $$
        SELECT P.*, F.PLATFORM_ID,
            EMAIL_PARSER(VALUE)['email'] PERSON_EMAIL, 
            EMAIL_PARSER(VALUE)['name'] PERSON_DISPLAY_NAME
        FROM STG.SFDC_EMAIL_MSTM.DIGEST_PERSON P
        LEFT JOIN REFERENCE.PLATFORM F ON P.PLATFORM = F.PLATFORM_NAME,
        LATERAL FLATTEN (INPUT => EMAIL_ADDRESS)
        $$
      ),
      (
        'SFDC',
        'EMAIL',
        'MSTM',
        'MESSAGE',
        $$['URL = \'s3://af-data-des-sandbox/ingest/jing/data2/\'', 'STORAGE_INTEGRATION = DES_INTEGRATION']$$,
        $$['TYPE = JSON']$$,
        $$[
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "SUBJECT",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "SENDER",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "BODY",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "REFERENCES",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "VARIANT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "SENT_AT",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TIMESTAMP_NTZ"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "PROBABLY_REPLY",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "BOOLEAN"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "AUTO_RESPONSE_TYPE",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "IN_REPLY_TO",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "ORIGINAL_ID",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "THREAD_ID",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "LOAD_TIME",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TIMESTAMP_NTZ"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "MESSAGE_ID",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "NTRL_SRC_STM_KEY",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "DATA_KEY",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "DATA_HASH",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            }
        ]$$,
        $$[
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "PLATFORM_ID",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "BUSINESS_RUN_DATE",
                "FIELD_TRANS": "CURRENT_DATE",
                "FIELD_TYPE": "DATE"
            }
        ]$$,
        $$
        SELECT M.*, F.PLATFORM_ID
        FROM STG.SFDC_EMAIL_MSTM.DIGEST_MESSAGE M
        LEFT JOIN REFERENCE.PLATFORM F
        ON M.PLATFORM = F.PLATFORM_NAME
        $$
      ),
      (
        'SFDC',
        'EMAIL',
        'MSTM',
        'MESSAGE_EMAIL',
        $$['URL = \'s3://af-data-des-sandbox/ingest/jing/data1/\'', 'STORAGE_INTEGRATION = DES_INTEGRATION']$$,
        $$['TYPE = JSON']$$,
        $$[
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "MESSAGE_ID",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "SENDER",
                "FIELD_TRANS": "EMAIL_PARSER(SENDER)['email']",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "RECIPIENT_EMAIL_ADDRESS",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "RECIPIENT_DISPLAY_NAME",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "VARIANT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "RECIPIENT_TYPE",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "LOAD_TIME",
                "FIELD_TRANS": "CURRENT_TIMESTAMP",
                "FIELD_TYPE": "TIMESTAMP_NTZ"
            }
        ]$$,
        $$[
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "MESSAGE_EMAIL_ID",
                "FIELD_TRANS": "_METADATA.SG_KEY_GEN.NEXTVAL",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "NTRL_SRC_STM_KEY",
                "FIELD_TRANS": "CONCAT({{KEY_LIST}})",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "DATA_KEY",
                "FIELD_TRANS": "MD5(CONCAT({{KEY_LIST}}))",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "DATA_HASH",
                "FIELD_TRANS": "MD5(TO_VARCHAR(ARRAY_CONSTRUCT({{HASH_LIST}})))",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "PLATFORM_ID",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "BUSINESS_RUN_DATE",
                "FIELD_TRANS": "CURRENT_DATE",
                "FIELD_TYPE": "DATE"
            }
        ]$$,
        $$
        SELECT M.*, F.PLATFORM_ID,
            EMAIL_PARSER(VALUE)['email'] RECIPIENT_EMAIL_ADDRESS, 
            EMAIL_PARSER(VALUE)['name'] RECIPIENT_DISPLAY_NAME,
            'TO' RECIPIENT_TYPE
        FROM STG.SFDC_EMAIL_MSTM.DIGEST_MESSAGE M
        LEFT JOIN REFERENCE.PLATFORM F ON M.PLATFORM = F.PLATFORM_NAME,
        LATERAL FLATTEN (INPUT => RECIPIENTS)
        $$
      ),
      (
        'SFDC',
        'EMAIL',
        'MSTM',
        'EMAIL_ADDRESS',
        $$['URL = \'s3://af-data-des-sandbox/ingest/jing/data1/\'', 'STORAGE_INTEGRATION = DES_INTEGRATION']$$,
        $$['TYPE = JSON']$$,
        $$[
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "EMAIL_ADDRESS",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "DISPLAY_NAME",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "VARIANT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "LOAD_TIME",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TIMESTAMP_NTZ"
            }
        ]$$,
        $$[
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "EMAIL_ADDRESS_ID",
                "FIELD_TRANS": "_METADATA.SG_KEY_GEN.NEXTVAL",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "NTRL_SRC_STM_KEY",
                "FIELD_TRANS": "CONCAT({{KEY_LIST}})",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "DATA_KEY",
                "FIELD_TRANS": "MD5(CONCAT({{KEY_LIST}}))",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "DATA_HASH",
                "FIELD_TRANS": "MD5(TO_VARCHAR(ARRAY_CONSTRUCT({{HASH_LIST}})))",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "PLATFORM_ID",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "BUSINESS_RUN_DATE",
                "FIELD_TRANS": "CURRENT_DATE",
                "FIELD_TYPE": "DATE"
            }
        ]$$,
        $$
        SELECT EMAIL_ADDRESS, F.PLATFORM_ID,
            ARRAY_AGG(DISPLAY_NAME) DISPLAY_NAME,
            MIN(CREATE_AT) CREATE_AT,
            MAX(LOAD_TIME) LOAD_TIME
        FROM (
            SELECT PLATFORM,
                EMAIL_PARSER(VALUE)['email'] EMAIL_ADDRESS, 
                EMAIL_PARSER(VALUE)['name'] DISPLAY_NAME,
                MIN(LOAD_TIME) CREATE_AT,
                MAX(LOAD_TIME) LOAD_TIME
             FROM (
                SELECT EMAIL_ADDRESS, PLATFORM, LOAD_TIME 
                FROM STG.SFDC_EMAIL_MSTM.DIGEST_PERSON
                UNION ALL
                SELECT ARRAY_APPEND(RECIPIENTS, SENDER), PLATFORM, LOAD_TIME
                FROM STG.SFDC_EMAIL_MSTM.DIGEST_MESSAGE
            ), LATERAL FLATTEN (INPUT => EMAIL_ADDRESS)
            GROUP BY 1,2,3
        ) A
        LEFT JOIN REFERENCE.PLATFORM F ON A.PLATFORM = F.PLATFORM_NAME
        GROUP BY 1,2
        $$
      )
) S
ON D.CLIENT_NAME = S.CLIENT_NAME
    AND D.PLATFORM_NAME = S.PLATFORM_NAME
    AND D.PLATFORM_TYPE = S.PLATFORM_TYPE
    AND D.DATA_CATALOG = S.DATA_CATALOG
    AND D.DATA_SCHEMA = S.DATA_SCHEMA
    AND D.DATA_NAME = S.DATA_NAME
WHEN MATCHED AND D.CONFIG_HASH != S.CONFIG_HASH THEN
    UPDATE SET
        DATA_STAGE = S.DATA_STAGE,
        DATA_FORMAT = S.DATA_FORMAT,
        CTRL_FIELD = S.CTRL_FIELD,
        DATA_FIELD = S.DATA_FIELD,
        META_FIELD = S.META_FIELD,
        TRANSFORMATION = S.TRANSFORMATION,
        CONFIG_HASH = S.CONFIG_HASH
WHEN NOT MATCHED THEN 
    INSERT (
        CLIENT_NAME,
        PLATFORM_NAME,
        PLATFORM_TYPE,
        DATA_CATALOG,
        DATA_SCHEMA,
        DATA_NAME,
        DATA_STAGE,
        DATA_FORMAT,
        CTRL_FIELD,
        DATA_FIELD,
        META_FIELD,
        TRANSFORMATION,
        CONFIG_HASH,
        AUTOMATED
    )
    VALUES (
        S.CLIENT_NAME,
        S.PLATFORM_NAME,
        S.PLATFORM_TYPE,
        S.DATA_CATALOG,
        S.DATA_SCHEMA,
        S.DATA_NAME,
        S.DATA_STAGE,
        S.DATA_FORMAT,
        S.CTRL_FIELD,
        S.DATA_FIELD,
        S.META_FIELD,
        S.TRANSFORMATION,
        S.CONFIG_HASH,
        S.AUTOMATED
    );

/*
--
-- Generate stage database schema
--
USE SCHEMA _METADATA;
SELECT * FROM CTRL_SCHEMA_UPDATE ORDER BY 1,2,3,4,5,6;
CALL CTRL_SCHEMA_UPDATER('DEBUG');
CALL CTRL_SCHEMA_UPDATER('WORK');
--INSERT INTO CTRL_CURRENT SELECT * FROM CTRL_IMPORT;




--
-- Goto Script-5 to generate some dummy raw data and check them flowing through
--




--
-- Demo for data source changes.
--
USE SCHEMA _METADATA;

DELETE FROM CTRL_IMPORT 
WHERE CLIENT_NAME = 'IH' 
AND PLATFORM_NAME = 'EMAIL'
--AND PLATFORM_TYPE = 'TYPE1'
AND DATA_NAME = 'MESSAGE';

SELECT * FROM CTRL_IMPORT;
SELECT * FROM CTRL_CURRENT;

SELECT * FROM CTRL_SCHEMA_UPDATE ORDER BY 1,2,3,4,5,6;
CALL CTRL_SCHEMA_UPDATER('DEBUG');
CALL CTRL_SCHEMA_UPDATER('WORK');

--
-- Modify stage database schema
--
SELECT * FROM CTRL_TASK_SCHEDULE;

USE SCHEMA _METADATA;
CALL CTRL_TASK_SCHEDULER('DATA_LOADER','DEBUG');
CALL CTRL_TASK_SCHEDULER('DATA_LOADER','WORK');
CALL CTRL_TASK_SCHEDULER('DATA_VERSION','DEBUG');
CALL CTRL_TASK_SCHEDULER('DATA_VERSION','WORK');
*/


/********************************************************************
 ** Schema Update Manually
 ********************************************************************/
USE SCHEMA INT._METADATA;

CALL CTRL_SCHEMA_UPDATER('WORK');

--CALL CTRL_TASK_SCHEDULER('DATA_LOADER','DEBUG');
--CALL CTRL_TASK_SCHEDULER('DATA_LOADER','WORK');



/********************************************************************
 ** Schema Update Manually
 ********************************************************************/
USE SCHEMA INT._METADATA;

--CALL CTRL_TASK_SCHEDULER('DATA_LOADER','DEBUG');
CALL CTRL_TASK_SCHEDULER('DATA_LOADER','WORK');

