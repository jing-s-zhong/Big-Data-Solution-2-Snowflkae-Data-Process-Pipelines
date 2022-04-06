-- 
-- default database should be INT or INTEGRATION
--
USE DATABASE INT;

/********************************************************************
 ** Data Pipeline Solution Deployment Section
 ********************************************************************/
--
-- execute context
--
DROP SCHEMA IF EXISTS _METADATA;
CREATE SCHEMA IF NOT EXISTS _METADATA;

--
-- create surogate key generator
--
DROP SEQUENCE IF EXISTS SG_KEY_GEN;
CREATE OR REPLACE SEQUENCE SG_KEY_GEN;
SELECT _METADATA.SG_KEY_GEN.NEXTVAL;

--
-- create data pipeline config import table
--
DROP TABLE IF EXISTS CTRL_IMPORT;
CREATE OR REPLACE TRANSIENT TABLE CTRL_IMPORT
(
    CLIENT_NAME VARCHAR,
    PLATFORM_NAME VARCHAR,
    PLATFORM_TYPE VARCHAR,
    DATA_CATALOG VARCHAR,
    DATA_SCHEMA VARCHAR,
    DATA_NAME VARCHAR,
    DATA_STAGE VARIANT,
    DATA_FORMAT VARIANT,
    CTRL_FIELD VARIANT,
    DATA_FIELD VARIANT,
    META_FIELD VARIANT,
    TRANSFORMATION VARCHAR,
    CONFIG_HASH VARCHAR,
    AUTOMATED BOOLEAN
);

--
-- create data pipeline config work table
--
DROP TABLE IF EXISTS CTRL_CURRENT;
CREATE OR REPLACE TRANSIENT TABLE CTRL_CURRENT CLONE CTRL_IMPORT;

--
-- create a processing log table
--
DROP TABLE IF EXISTS CTRL_LOG;
CREATE OR REPLACE TRANSIENT TABLE CTRL_LOG
(
	EVENT_ID NUMBER NOT NULL IDENTITY,
	EVENT_TIME TIMESTAMP_NTZ DEFAULT TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP),
	EVENT_STATUS TEXT,
	EVENT_MESSAGE TEXT,
	EVENT_QUERY TEXT
);

--
-- data pipeline state transition view
--
DROP VIEW IF EXISTS CTRL_SCHEMA_UPDATE;
CREATE OR REPLACE VIEW CTRL_SCHEMA_UPDATE AS
WITH SQL_SYNTAX_TEMPLATES AS ( -- SQL syntax templates
  SELECT $1 TEMPLATE_ID, $2 TEMPLATE_TEXT FROM VALUES
    (101, 'CREATE OR REPLACE TRANSIENT SCHEMA /*IF NOT EXISTS*/ {{DATA_SCHEMA}};'),
    (102, 'GRANT USAGE ON SCHEMA {{DATA_SCHEMA}} TO ROLE LOADER;'),
    --(201, 'CREATE OR REPLACE FILE FORMAT {{DATA_SCHEMA}}.FORMAT_{{DATA_NAME}} \n\t{{DATA_FORMAT}};'),
    (202, 'CREATE OR REPLACE TABLE {{DATA_SCHEMA}}.RAW_{{DATA_NAME}} (\n\t{{SOURCE_FILED_DEF}}\n);'),
    --(203, 'CREATE OR REPLACE STAGE {{DATA_SCHEMA}}.STAGE_{{DATA_NAME}} \n\tFILE_FORMAT = {{DATA_SCHEMA}}.FORMAT_{{DATA_NAME}} \n\t{{DATA_STAGE}};'), 
    (204, 'CREATE OR REPLACE STREAM {{DATA_SCHEMA}}.STREAM_{{DATA_NAME}} \nON TABLE {{DATA_SCHEMA}}.RAW_{{DATA_NAME}};'),
    (205, 'CREATE OR REPLACE TABLE {{DATA_SCHEMA}}.{{DATA_NAME}} (\n\t{{TARGET_FILED_DEF}}\n);'),
    (206, 'CREATE OR REPLACE TABLE {{DATA_SCHEMA}}.XREF_{{DATA_NAME}} (\n\t{{XREF_FIELD_DEF}} \n);'),
    (207, 'CREATE OR REPLACE VIEW {{DATA_SCHEMA}}.DIGEST_{{DATA_NAME}} AS /* TODO: tweak this view */ \nWITH PARTITIONED AS ( \n\tSELECT *, ROW_NUMBER() OVER (PARTITION BY {{DATA_KEY_FIELD}} ORDER BY {{DATA_TIME_FIELD}} DESC) MOST_RECENT \n\tFROM {{DATA_SCHEMA}}.{{DATA_NAME}}\n), \nDIGESTED AS ( \n\tSELECT P.*, \n\t\tCASE WHEN R.{{DATA_KEY_FIELD}} IS NULL THEN \'INSERT\' \n\t\tWHEN R.{{DATA_HASH_FIELD}} != P.{{DATA_HASH_FIELD}} THEN \'UPDATE\' \n\t\tELSE NULL END DIGEST \n\tFROM PARTITIONED P \n\tLEFT JOIN {{DATA_SCHEMA}}.XREF_{{DATA_NAME}} R \n\tON P.{{DATA_KEY_FIELD}} = R.{{DATA_KEY_FIELD}} \n\tWHERE P.MOST_RECENT = 1 \n\t) \nSELECT * \nFROM DIGESTED \nWHERE DIGEST IS NOT NULL\n;'),
    (301, 'GRANT ALL PRIVILEGES ON {{DATA_SCHEMA}}.RAW_{{DATA_NAME}} TO ROLE LOADER;'),
    --(302, 'GRANT ALL PRIVILEGES ON {{DATA_SCHEMA}}.STAGE_{{DATA_NAME}} TO ROLE LOADER;'),
    (401, 'DROP VIEW IF EXISTS {{DATA_SCHEMA}}.DIGEST_{{DATA_NAME}};'),
    (402, 'DROP TABLE IF EXISTS {{DATA_SCHEMA}}.XREF_{{DATA_NAME}};'),
    (403, 'DROP TABLE IF EXISTS {{DATA_SCHEMA}}.{{DATA_NAME}};'),
    (404, 'DROP STREAM IF EXISTS {{DATA_SCHEMA}}.STREAM_{{DATA_NAME}};'),
    --(405, 'DROP STAGE IF EXISTS {{DATA_SCHEMA}}.STAGE_{{DATA_NAME}};'),
    (406, 'DROP TABLE IF EXISTS {{DATA_SCHEMA}}.RAW_{{DATA_NAME}};'),
    --(407, 'DROP FILE FORMAT IF EXISTS {{DATA_SCHEMA}}.FORMAT_{{DATA_NAME}};'),
    (501, 'DROP SCHEMA IF EXISTS {{DATA_SCHEMA}};')
),
FUNCTIONAL_CODE_BLOCKS AS ( -- data pipeline DDL code blocks
  SELECT CODE_BLOCK_ID, DEPLOYMENT_TYPE, PIPELINE_OPERATION_TYPE, ARRAY_AGG(TRY_TO_NUMBER(VALUE)) WITHIN GROUP (ORDER BY INDEX) CODE_SYNTAX_ARRAY
  FROM ( SELECT $1 CODE_BLOCK_ID, $2 DEPLOYMENT_TYPE, $3 PIPELINE_OPERATION_TYPE, $4 CODE_SYNTAX_TEMPLATE_REF FROM VALUES
    (10, 'INGESTION', 'PIPELINE_REMOVE', CONCAT_WS(',', -- Remove a data ingestion pipeline
        '401', --Drop digest view
        '402', --Drop xref table
        '403', --Drop data table
        '404', --Drop raw data stream
        --'405', --Drop raw data stage
        '406', --Drop raw data table
        --'407'  --Drop data file format
        '')),
    (11, 'INGESTION', 'PIPELINE_CREATE', CONCAT_WS(',', -- Create a data ingestion pipeline
        --'201', --Create data file format
        '202', --Create raw data table
        --'203', --Create raw data stage
        '204', --Create raw data stream
        '205', --Create data table
        '206', --Create xref table
        '207', --Create digest view
        '301', --Grant raw table permission
        --'302', --Grant raw stage permission
        '')),
    (20, 'INTEGRATION', 'PIPELINE_REMOVE', CONCAT_WS(',', -- Remove a data integration pipeline
        '401', --Drop digest view
        '402', --Drop xref table
        '403', --Drop data table
        '404',  --Drop raw data stream
        '')),
    (21, 'INTEGRATION', 'PIPELINE_CREATE', CONCAT_WS(',', -- Create a data integration pipeline
        '205', --Create data table
        '206', --Create xref table
        '207', --Create digest view
        ''))
  ),
  LATERAL SPLIT_TO_TABLE (CODE_SYNTAX_TEMPLATE_REF, ',')
  GROUP BY 1,2,3
  ORDER BY 1
),
PIPELINE_DEPLOYMENT_TYPES AS ( -- data pipeline operation types
  SELECT $1 DEPLOYMENT_TYPE_ID, $2 DEPLOYMENT_TYPE, SPLIT($3, ',') DEPLOYMENT_TARGET_DATABASE FROM VALUES
    (10, 'INGESTION', CONCAT_WS(',', -- Deploy to stage for data ingestion
        'STG', -- stage db name list
        'STAGE',
        'STAGING',
        '')),
    (20, 'INTEGRATION', CONCAT_WS(',', -- Deploy to integration for data integration
        'INT', -- integration db name list
        'INTEGRATION',
        ''))
),
PIPELINE_OPERATION_STATES AS (
    SELECT -- Find the data source changes by comparing two copies of the config data
        IFNULL(S.CLIENT_NAME, C.CLIENT_NAME) CLIENT_NAME,
        IFNULL(S.PLATFORM_NAME, C.PLATFORM_NAME) PLATFORM_NAME,
        IFNULL(S.PLATFORM_TYPE, C.PLATFORM_TYPE) PLATFORM_TYPE,
        IFNULL(S.DATA_CATALOG, C.DATA_CATALOG) DATA_CATALOG_OVERALL,
        IFNULL(S.DATA_SCHEMA, C.DATA_SCHEMA) DATA_SCHEMA_OVERALL,
        IFNULL(S.DATA_NAME, C.DATA_NAME) DATA_NAME,
        IFNULL(ARRAY_TO_STRING(S.DATA_STAGE, ', \n\t'), '') DATA_STAGE,
        IFNULL(ARRAY_TO_STRING(S.DATA_STAGE, ', \n\t'), '') DATA_STAGE,
        IFNULL(ARRAY_TO_STRING(S.DATA_FORMAT, ', \n\t'), '') DATA_FORMAT,
        IFNULL(S.CTRL_FIELD:DATA_KEY_FIELD, 'DATA_KEY') DATA_KEY_FIELD,
        IFNULL(S.CTRL_FIELD:DATA_HASH_FIELD, 'DATA_HASH') DATA_HASH_FIELD,
        IFNULL(S.CTRL_FIELD:DATA_TIME_FIELD, 'DATA_TIME') DATA_TIME_FIELD,
        IFNULL(ARRAY_TO_STRING(S.TARGET_FILED, ', \n\t'), '') TARGET_FILED_DEF,
        IFNULL(ARRAY_TO_STRING(S.SOURCE_FILED, ', \n\t'), '') SOURCE_FILED_DEF,
        IFNULL(ARRAY_TO_STRING(S.XREF_FIELD, ', \n\t'), '') XREF_FIELD_DEF,
        S.CONFIG_HASH, -- leave this as is, we need it to compute the states
        -- Compute current state of the pipeline Moore machine
        IFF(C.CONFIG_HASH IS NULL, TRUE, FALSE) PIPELINE_CREATE_STATE,
        IFF((S.CONFIG_HASH IS NULL AND C.AUTOMATED), TRUE, FALSE) PIPELINE_REMOVE_STATE, -- two conditions: (1)user request removal; (2)automation allowed
        IFF((C.CONFIG_HASH != S.CONFIG_HASH AND C.AUTOMATED), TRUE, FALSE) PIPELINE_CHANGE_STATE, -- two conditions: (1)user changed config; (2)automation allowed
        -- Compute current state of the container Moore machine
        IFF(SUM(1) OVER(PARTITION BY DATA_SCHEMA_OVERALL) = SUM(IFF(PIPELINE_CREATE_STATE, 1, 0)) OVER(PARTITION BY DATA_SCHEMA_OVERALL), TRUE, FALSE) CONTAINER_CREATE,
        IFF(SUM(1) OVER(PARTITION BY DATA_SCHEMA_OVERALL) = SUM(IFF(PIPELINE_REMOVE_STATE, 1, 0)) OVER(PARTITION BY DATA_SCHEMA_OVERALL), TRUE, FALSE) CONTAINER_REMOVE,
        --CASE WHEN CONTAINER_CREATE THEN 'CONTAINER_CREATE' WHEN CONTAINER_REMOVE THEN 'CONTAINER_REMOVE' END CONTAINER_OPERATION_TYPE
        CASE 
            WHEN PIPELINE_CREATE_STATE OR PIPELINE_CHANGE_STATE THEN 'PIPELINE_CREATE' -- ingestion pipeline create block ref
            WHEN PIPELINE_REMOVE_STATE THEN 'PIPELINE_REMOVE' -- ingestion pipeline remove block ref
            ELSE 'NONE'
        END PIPELINE_OPERATION_TYPE
    FROM CTRL_CURRENT C
    FULL JOIN (
        SELECT -- Interpret newest config
            CONFIG_HASH,
            CLIENT_NAME,
            PLATFORM_NAME,
            PLATFORM_TYPE,
            IFNULL(DATA_CATALOG, CURRENT_DATABASE()) DATA_CATALOG,
            DATA_SCHEMA,
            DATA_NAME,
            DATA_STAGE,
            DATA_FORMAT,
            CTRL_FIELD,
            ARRAY_SIZE(DATA_FIELD) RAW_SIZE,
            ARRAY_AGG(CONCAT(B.VALUE:FIELD_NAME::VARCHAR, ' ', B.VALUE:FIELD_TYPE::VARCHAR)) WITHIN GROUP (ORDER BY INDEX) TARGET_FILED,
            ARRAY_AGG(IFF(B.INDEX < RAW_SIZE, CONCAT(B.VALUE:FIELD_NAME::VARCHAR, ' ', B.VALUE:FIELD_TYPE::VARCHAR), NULL)) WITHIN GROUP (ORDER BY INDEX) SOURCE_FILED,
            ARRAY_AGG(IFF(B.VALUE:FIELD_FOR_XREF::BOOLEAN, CONCAT(B.VALUE:FIELD_NAME::VARCHAR, ' ', B.VALUE:FIELD_TYPE::VARCHAR), NULL)) WITHIN GROUP (ORDER BY INDEX) XREF_FIELD
        FROM CTRL_IMPORT A, 
        LATERAL FLATTEN( INPUT => ARRAY_CAT(DATA_FIELD, IFNULL(META_FIELD, ARRAY_CONSTRUCT()))) B
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11
    ) S
    ON C.CLIENT_NAME = S.CLIENT_NAME
        AND C.PLATFORM_NAME = S.PLATFORM_NAME
        AND C.PLATFORM_TYPE = S.PLATFORM_TYPE
        AND C.DATA_SCHEMA = S.DATA_SCHEMA
        AND C.DATA_NAME = S.DATA_NAME
),
PIPELINE_OPERATION_CASES AS ( -- Apply object update teamplate
    SELECT CLIENT_NAME, 
        PLATFORM_NAME,
        PLATFORM_TYPE,
        DATA_SCHEMA_OVERALL,
        DATA_NAME,
        ARRAY_AGG(PIPELINE_OPERATION_DDL) WITHIN GROUP (ORDER BY INDEX) PIPELINE_OPERATION_SEQUENCE,
        ROW_NUMBER() OVER(PARTITION BY DATA_SCHEMA_OVERALL ORDER BY DATA_NAME) CONTAINER_CREATE_BIND,
        ROW_NUMBER() OVER(PARTITION BY DATA_SCHEMA_OVERALL ORDER BY DATA_NAME DESC) CONTAINER_REMOVE_BIND
    FROM (
        SELECT POS.*, PDT.DEPLOYMENT_TYPE_ID, FCB.CODE_BLOCK_ID, CSR.INDEX, SST.TEMPLATE_ID,
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(SST.TEMPLATE_TEXT, 
                '{{DATA_SCHEMA}}', DATA_SCHEMA_OVERALL),
                '{{DATA_NAME}}', DATA_NAME),
                '{{DATA_FORMAT}}', DATA_FORMAT),
                '{{DATA_KEY_FIELD}}', DATA_KEY_FIELD),
                '{{DATA_HASH_FIELD}}', DATA_HASH_FIELD),
                '{{DATA_TIME_FIELD}}', DATA_TIME_FIELD),
                '{{XREF_FIELD_DEF}}', XREF_FIELD_DEF),
                '{{SOURCE_FILED_DEF}}', SOURCE_FILED_DEF),
                '{{TARGET_FILED_DEF}}', TARGET_FILED_DEF) PIPELINE_OPERATION_DDL
        FROM PIPELINE_OPERATION_STATES POS
        LEFT JOIN PIPELINE_DEPLOYMENT_TYPES PDT
        ON ARRAY_CONTAINS(POS.DATA_CATALOG_OVERALL::VARIANT, PDT.DEPLOYMENT_TARGET_DATABASE)
        LEFT JOIN FUNCTIONAL_CODE_BLOCKS FCB
        ON POS.PIPELINE_OPERATION_TYPE = FCB.PIPELINE_OPERATION_TYPE
        AND PDT.DEPLOYMENT_TYPE = FCB.DEPLOYMENT_TYPE,
        LATERAL FLATTEN( INPUT => FCB.CODE_SYNTAX_ARRAY ) CSR
        JOIN SQL_SYNTAX_TEMPLATES SST
        ON CSR.VALUE = SST.TEMPLATE_ID
    )
    GROUP BY 1,2,3,4,5
),
CONTAINER_OPERATION_CASES AS ( -- Apply schema create teamplate
    SELECT DATA_SCHEMA_OVERALL, CONTAINER_CREATE, CONTAINER_REMOVE,
        ARRAY_AGG(CONTAINER_OPERATION_DDL) WITHIN GROUP (ORDER BY INDEX) CONTAINER_OPERATION_SEQUENCE
    FROM (
        SELECT COS.*, COT.*, SST.TEMPLATE_TEXT, REPLACE(SST.TEMPLATE_TEXT, '{{DATA_SCHEMA}}', DATA_SCHEMA_OVERALL) CONTAINER_OPERATION_DDL
        FROM (
            SELECT DATA_SCHEMA_OVERALL, CONTAINER_CREATE, CONTAINER_REMOVE, 
                CASE
                    WHEN CONTAINER_CREATE THEN ARRAY_CONSTRUCT(101,102) --Create schema and grant permisson
                    WHEN CONTAINER_REMOVE THEN ARRAY_CONSTRUCT(501) --Drop the empty schema
                    ELSE ARRAY_CONSTRUCT() -- Not do anything with schema
                END CONTAINER_OPERATION_BLOCK
            FROM PIPELINE_OPERATION_STATES
            WHERE CONTAINER_CREATE OR CONTAINER_REMOVE
            GROUP BY 1,2,3
        ) COS,
        LATERAL FLATTEN( INPUT => CONTAINER_OPERATION_BLOCK ) COT
        JOIN SQL_SYNTAX_TEMPLATES SST
        ON COT.VALUE = SST.TEMPLATE_ID
    )
    GROUP BY 1,2,3
)
SELECT -- Attach schema change sub-cases to the right object change case
    POC.CLIENT_NAME, 
    POC.PLATFORM_NAME,
    POC.PLATFORM_TYPE,
    POC.DATA_SCHEMA_OVERALL DATA_SCHEMA,
    POC.DATA_NAME,
    CONTAINER_CREATE_BIND OPERATION_ORDER,
    ARRAY_CAT(ARRAY_CAT( -- Compute the state output
        IFF(COC.CONTAINER_CREATE AND POC.CONTAINER_CREATE_BIND = 1, COC.CONTAINER_OPERATION_SEQUENCE, ARRAY_CONSTRUCT_COMPACT()), 
        IFNULL(POC.PIPELINE_OPERATION_SEQUENCE, ARRAY_CONSTRUCT_COMPACT())), 
        IFF(COC.CONTAINER_REMOVE AND POC.CONTAINER_REMOVE_BIND = 1, COC.CONTAINER_OPERATION_SEQUENCE, ARRAY_CONSTRUCT_COMPACT())
    ) METADATA_UPDATE
FROM PIPELINE_OPERATION_CASES POC
LEFT JOIN CONTAINER_OPERATION_CASES COC
ON POC.DATA_SCHEMA_OVERALL = COC.DATA_SCHEMA_OVERALL
--ORDER BY POC.DATA_SCHEMA_OVERALL, POC.DATA_NAME, OPERATION_ORDER
;

--
-- data pipeline processing schedule view
--
DROP VIEW IF EXISTS CTRL_TASK_SCHEDULE;
CREATE OR REPLACE VIEW CTRL_TASK_SCHEDULE AS
WITH SQL_SYNTAX_TEMPLATES AS ( -- used SQL syntax templates
SELECT $1 TEMPLATE_ID, $2 TEMPLATE_TEXT
FROM VALUES
(1, '
MERGE INTO {{DATA_SCHEMA}}.{{DATA_NAME}} D 
USING ( 
  SELECT *
  FROM (
    SELECT {{SELECT_LIST}},
        ROW_NUMBER() OVER (PARTITION BY {{DATA_KEY_FIELD}} ORDER BY {{DATA_TIME_FIELD}} DESC) PICKER
    FROM {{SOURCE_DATA}} 
  )
  WHERE PICKER = 1
) S 
ON D.{{DATA_KEY_FIELD}} = S.{{DATA_KEY_FIELD}} 
/*WHEN MATCHED AND S.{{DATA_HASH_FIELD}} IS NULL THEN DELETE*/ 
WHEN MATCHED AND D.{{DATA_HASH_FIELD}} != S.{{DATA_HASH_FIELD}} THEN 
UPDATE SET 
	{{UPDATE_LIST}} 
WHEN NOT MATCHED THEN 
INSERT (
	{{FIELD_LIST}}
) 
VALUES (
	{{VALUE_LIST}}
);
'),
(2, '
MERGE INTO {{DATA_SCHEMA}}.XREF_{{DATA_NAME}} D 
USING ( 
  SELECT *
  FROM (
	SELECT {{SELECT_XREF}},
        ROW_NUMBER() OVER (PARTITION BY {{DATA_KEY_FIELD}} ORDER BY {{DATA_TIME_FIELD}} DESC) PICKER
	FROM {{DATA_SCHEMA}}.{{DATA_NAME}} 
  )
  WHERE PICKER = 1
) S 
ON D.{{DATA_KEY_FIELD}} = S.{{DATA_KEY_FIELD}} 
WHEN MATCHED AND D.{{DATA_HASH_FIELD}} != S.{{DATA_HASH_FIELD}} THEN 
UPDATE SET 
	{{UPDATE_XREF}} 
WHEN NOT MATCHED THEN 
INSERT (
	{{FIELD_XREF}}
) 
VALUES (
	{{VALUE_XREF}}
);
')
),
JINJA_EXPRESSIONS AS ( -- get all conditions from db and compute all expressions
    SELECT CLIENT_NAME,
        PLATFORM_NAME,
        PLATFORM_TYPE,
        DATA_SCHEMA,
        DATA_NAME,
        IFNULL(CTRL_FIELD:DATA_KEY_FIELD,  'DATA_KEY') DATA_KEY_FIELD,
        IFNULL(CTRL_FIELD:DATA_HASH_FIELD, 'DATA_HASH') DATA_HASH_FIELD,
        IFNULL(CTRL_FIELD:DATA_TIME_FIELD, 'DATA_TIME') DATA_TIME_FIELD,
        IFF(IFNULL(TRANSFORMATION, '') = '', CONCAT('{{DATA_SCHEMA}}.STREAM_', DATA_NAME), CONCAT ('(\n\t\t/* transformation begin */', TRANSFORMATION, '/* transformation end */\n\t)')) SOURCE_DATA,
        -- data field
        FIELD_LIST,
        UPDATE_LIST,
        VALUE_LIST,
        REPLACE(REPLACE( SELECT_LIST, '{{KEY_LIST}}', KEY_LIST), '{{HASH_LIST}}', HASH_LIST) SELECT_LIST,
        -- xref field
        FIELD_XREF,
        VALUE_XREF,
        UPDATE_XREF,
        REPLACE(REPLACE( SELECT_XREF, '{{KEY_LIST}}', KEY_LIST), '{{HASH_LIST}}', HASH_LIST) SELECT_XREF,
        -- key & hash field
        KEY_LIST,
        HASH_LIST
    FROM (
        SELECT
            CLIENT_NAME,
            PLATFORM_NAME,
            PLATFORM_TYPE,
            DATA_SCHEMA,
            DATA_NAME,
            CTRL_FIELD,
            TRANSFORMATION,
            -- data field
            ARRAY_TO_STRING(ARRAY_AGG(FIELD_LIST_ITEM) WITHIN GROUP (ORDER BY INDEX), ', \n\t') FIELD_LIST,
            ARRAY_TO_STRING(ARRAY_AGG(VALUE_LIST_ITEM) WITHIN GROUP (ORDER BY INDEX), ', \n\t') VALUE_LIST,
            ARRAY_TO_STRING(ARRAY_AGG(UPDATE_LIST_ITEM) WITHIN GROUP (ORDER BY INDEX), ', \n\t') UPDATE_LIST,
            ARRAY_TO_STRING(ARRAY_AGG(SELECT_LIST_ITEM) WITHIN GROUP (ORDER BY INDEX), ', \n\t\t') SELECT_LIST,
            -- xref field
            ARRAY_TO_STRING(ARRAY_AGG(FIELD_XREF_ITEM) WITHIN GROUP (ORDER BY INDEX), ', \n\t') FIELD_XREF,
            ARRAY_TO_STRING(ARRAY_AGG(VALUE_XREF_ITEM) WITHIN GROUP (ORDER BY INDEX), ', \n\t') VALUE_XREF,
            ARRAY_TO_STRING(ARRAY_AGG(UPDATE_XREF_ITEM) WITHIN GROUP (ORDER BY INDEX), ', \n\t') UPDATE_XREF,
            ARRAY_TO_STRING(ARRAY_AGG(SELECT_XREF_ITEM) WITHIN GROUP (ORDER BY INDEX), ', \n\t\t') SELECT_XREF,
            -- key & hash field
            ARRAY_TO_STRING(ARRAY_AGG(DATA_KEY_ITEM) WITHIN GROUP (ORDER BY INDEX), ', ''_'', ') KEY_LIST,
            ARRAY_TO_STRING(ARRAY_AGG(DATA_HASH_ITEM) WITHIN GROUP (ORDER BY INDEX), ', ') HASH_LIST
        FROM (
            SELECT A.*, 
                B.INDEX,
                REPLACE(REPLACE(REPLACE(REPLACE(IFNULL(B.VALUE:FIELD_TRANS, ''),
                  '{{CLIENT_NAME}}', CONCAT('''', CLIENT_NAME, '''')),
                  '{{PLATFORM_NAME}}', CONCAT('''', PLATFORM_NAME, '''')),
                  '{{PLATFORM_TYPE}}', CONCAT('''', PLATFORM_TYPE, '''')),
                  '{{DATA_NAME}}', CONCAT('''', DATA_NAME, '''')) FIELD_TRANS,
                -- data field
                B.VALUE:FIELD_NAME::VARCHAR FIELD_LIST_ITEM,
                CONCAT('{{S.}}', B.VALUE:FIELD_NAME) VALUE_LIST_ITEM,
                CONCAT(FIELD_LIST_ITEM, ' = {{S.}}', B.VALUE:FIELD_NAME) UPDATE_LIST_ITEM,
                CONCAT(REPLACE(FIELD_TRANS, '?', B.VALUE:FIELD_NAME), ' ', FIELD_LIST_ITEM) SELECT_LIST_ITEM,
                -- xref field
                IFF(B.VALUE:FIELD_FOR_XREF::BOOLEAN, FIELD_LIST_ITEM, NULL) FIELD_XREF_ITEM,
                IFF(B.VALUE:FIELD_FOR_XREF::BOOLEAN, CONCAT('{{S.}}', B.VALUE:FIELD_NAME), NULL) VALUE_XREF_ITEM,
                IFF(B.VALUE:FIELD_FOR_XREF::BOOLEAN, CONCAT(FIELD_LIST_ITEM, ' = {{S.}}', B.VALUE:FIELD_NAME), NULL) UPDATE_XREF_ITEM,
                IFF(B.VALUE:FIELD_FOR_XREF::BOOLEAN, FIELD_LIST_ITEM, NULL) SELECT_XREF_ITEM,
                -- key & hash field
                IFF(B.VALUE:FIELD_FOR_KEY::BOOLEAN, FIELD_LIST_ITEM, NULL) DATA_KEY_ITEM,
                IFF(B.VALUE:FIELD_FOR_HASH::BOOLEAN, FIELD_LIST_ITEM, NULL) DATA_HASH_ITEM
            FROM CTRL_CURRENT A, 
            LATERAL FLATTEN( INPUT => ARRAY_CAT(DATA_FIELD, IFNULL(META_FIELD, ARRAY_CONSTRUCT())) ) B
        )
        GROUP BY 1,2,3,4,5,6,7
    )
),
JINJA_REDENDER AS (
    SELECT JE.DATA_SCHEMA, JE.DATA_NAME, 
        ARRAY_AGG(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(SST.TEMPLATE_TEXT,
          '{{SELECT_XREF}}', SELECT_XREF),
          '{{UPDATE_XREF}}', UPDATE_XREF),
          '{{FIELD_XREF}}', FIELD_XREF),
          '{{VALUE_XREF}}', VALUE_XREF), 
          '{{SELECT_LIST}}', SELECT_LIST),
          '{{UPDATE_LIST}}', UPDATE_LIST),
          '{{FIELD_LIST}}', FIELD_LIST),
          '{{VALUE_LIST}}', VALUE_LIST), 
          '{{DATA_NAME}}', DATA_NAME),
          '{{DATA_KEY_FIELD}}', DATA_KEY_FIELD),
          '{{DATA_HASH_FIELD}}', DATA_HASH_FIELD),
          '{{DATA_TIME_FIELD}}', DATA_TIME_FIELD),
          '{{SOURCE_DATA}}', SOURCE_DATA),
          '{{STAGE_DIGEST}}', CONCAT('DIGEST_', DATA_NAME)),
          '{{DATA_STREAM}}', CONCAT('STREAM_', DATA_NAME)),
          '{{DATA_SCHEMA}}', DATA_SCHEMA), -- let this line stay at the bottom
          '{{S.}}', 'S.')) WITHIN GROUP (ORDER BY SST.TEMPLATE_ID) JINJA_REDENDED
    FROM JINJA_EXPRESSIONS JE
    JOIN SQL_SYNTAX_TEMPLATES SST
    ON TRUE
    GROUP BY 1,2
)
SELECT DATA_SCHEMA, 
    DATA_NAME, 
    JINJA_REDENDED[0] DATA_LOADER, 
    JINJA_REDENDED[1] DATA_VERSION
FROM JINJA_REDENDER
;

--
-- email address and display name parser
--
DROP FUNCTION IF EXISTS EMAIL_PARSER(VARCHAR);
CREATE OR REPLACE FUNCTION EMAIL_PARSER(EMAIL VARCHAR)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
STRICT
AS '
function display_name(text) {
    return text.replace(/(^[\s,>]+)|"|([\s,<]+$)/g, "");
}
var email_regex = /[a-zA-Z0-9.!#$%&''*+\/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*/g;
if (match = email_regex.exec(EMAIL)) {
    var dispalay_name = display_name(EMAIL.substring(0, match["index"]));
}
return {"name": dispalay_name, "email": match[0]};
';
/* SELECT EMAIL_PARSER('Abert Jameson <abert.jameson@example.com>'); */

--
-- data pileline state transition procedure
--
DROP PROCEDURE IF EXISTS CTRL_SCHEMA_UPDATER(VARCHAR);
CREATE OR REPLACE PROCEDURE CTRL_SCHEMA_UPDATER(
    CALL_MODE VARCHAR -- WORK or DEBUG otherwise
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS $$
    var loggerQuery = 'INSERT INTO CTRL_LOG (EVENT_STATUS, EVENT_MESSAGE, EVENT_QUERY) VALUES(:1, :2, :3);';
    var loggerStatus = '', loggerMessage = '', loggerMode = 'VERBOSE', result = '';
    try {
        var updateQuery = 'SELECT *, CURRENT_SCHEMA() METADATA_SCHEMA FROM CTRL_SCHEMA_UPDATE ORDER BY 1,2,3,4,5,6;';
        var updateStmnt = snowflake.createStatement ({ sqlText: updateQuery });
        var updateReslt = updateStmnt.execute();
        //result += updateQuery + '\n';
        var updateCount = 0;
        while (updateReslt.next()) {
            var clientName = updateReslt.getColumnValue("CLIENT_NAME");
            var platformName = updateReslt.getColumnValue("PLATFORM_NAME");
            var platformType = updateReslt.getColumnValue("PLATFORM_TYPE");
            var dataSchema = updateReslt.getColumnValue("DATA_SCHEMA");
            var dataName = updateReslt.getColumnValue("DATA_NAME");
            var metadataUpdate = updateReslt.getColumnValue("METADATA_UPDATE");
            var metadataSchema = updateReslt.getColumnValue("METADATA_SCHEMA");
            //result += JSON.stringify(metadataUpdate) + '\n';
            for (var i = 0; i < metadataUpdate.length; i++) {
                var updateDDL = metadataUpdate[i];
                var updateCMD = snowflake.createStatement ({ sqlText: updateDDL });
                if (CALL_MODE == "WORK") {
                    try {
                        var updateRET = updateCMD.execute();
                        loggerStatus = 'SUCCEDDED';
                        loggerMessage = '';
                    }
                    catch (err1) {
                        loggerStatus = 'FAILED';
                        loggerMessage = err1.toString();
                    }
                    finally {
                        if (loggerMode == 'ERROR_ONLY' && loggerStatus == 'SUCCEDDED') continue;
                        var loggerStmnt = snowflake.createStatement ({ sqlText: loggerQuery, binds: [loggerStatus, loggerMessage, updateDDL]});
                        var loggerRsult = loggerStmnt.execute();
                    }
                }
                result += updateDDL + '\n';
                updateCount++;
            }
        }
        if (updateCount) {
            var refreshQueries = [
                '\nDELETE FROM ' + metadataSchema + '.CTRL_CURRENT WHERE AUTOMATED = TRUE;', 
                'INSERT INTO ' + metadataSchema + '.CTRL_CURRENT SELECT * FROM ' + metadataSchema + '.CTRL_IMPORT; /* TODO: filter out automation disabled rows */'
                ];
            for (var i = 0; i < refreshQueries.length; i++) {
                var refreshQuery = refreshQueries[i];
                var refreshStmnt = snowflake.createStatement ({ sqlText: refreshQuery });
                if (CALL_MODE == "WORK") {
                    try {
                        var refreshReslt = refreshStmnt.execute();
                        loggerStatus = 'SUCCEDDED';
                        loggerMessage = '';
                    }
                    catch (err1) {
                        loggerStatus = 'FAILED';
                        loggerMessage = err1.toString();
                    }
                    finally {
                        if (loggerMode == 'ERROR_ONLY' && loggerStatus == 'SUCCEDDED') continue;
                        var loggerStmnt = snowflake.createStatement ({ sqlText: loggerQuery, binds: [loggerStatus, loggerMessage, refreshQuery]});
                        var loggerRsult = loggerStmnt.execute();
                    }
                }
                result += refreshQuery + '\n';
            }
        }
        else {
            result += 'No change!'
        }
    }
    catch (err) {
        result += err.toString() + '\n';
        loggerStatus = 'FAILED';
        loggerMessage = err1.toString();
        var loggerStmnt = snowflake.createStatement ({ sqlText: loggerQuery, binds: [loggerStatus, loggerMessage, "CALL CTRL_SCHEMA_UPDATER(...)"]});
        var loggerRsult = loggerStmnt.execute();
    }
    return result;
$$;

--
-- data pileline processing schedule procedure
--
DROP PROCEDURE IF EXISTS CTRL_TASK_SCHEDULER(VARCHAR, VARCHAR);
CREATE OR REPLACE PROCEDURE CTRL_TASK_SCHEDULER (
    TASK_MODE VARCHAR, -- DATA_LOADER or DATA_VERSION
    CALL_MODE VARCHAR -- WORK or DEBUG otherwise
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS $$
    var loggerQuery = 'INSERT INTO CTRL_LOG (EVENT_STATUS, EVENT_MESSAGE, EVENT_QUERY) VALUES(:1, :2, :3);';
    var loggerStatus = '', loggerMessage = '', loggerMode = 'VERBOSE', result = '';
    try {
        var scheduleQuery = 'SELECT * FROM CTRL_TASK_SCHEDULE;';
        var scheduleStmnt = snowflake.createStatement ({ sqlText: scheduleQuery });
        var scheduleReslt = scheduleStmnt.execute();
        //result += scheduleQuery + '\n';
        while (scheduleReslt.next()) {
            var dataSchema = scheduleReslt.getColumnValue("DATA_SCHEMA");
            var dataName = scheduleReslt.getColumnValue("DATA_NAME");
            var mergeQuery = scheduleReslt.getColumnValue(TASK_MODE);
            //result += JSON.stringify(mergeQuery) + '\n';
            var scheduleDML = mergeQuery;
            var scheduleCMD = snowflake.createStatement ({ sqlText: scheduleDML });
            var loggerStatus = '', loggerMessage = '';
            if (CALL_MODE == "WORK") {
                try {
                    var scheduleRET = scheduleCMD.execute();
                    loggerStatus = 'SUCCEDDED';
                    loggerMessage = '';
                }
                catch (err1) {
                    loggerStatus = 'FAILED';
                    loggerMessage = err1.toString();
                }
                finally {
                    if (loggerMode == 'ERROR_ONLY' && loggerStatus == 'SUCCEDDED') continue;
                    var logStmnt = snowflake.createStatement ({ sqlText: loggerQuery, binds: [loggerStatus, loggerMessage, scheduleDML]});
                    var logRsult = logStmnt.execute();
                }
            }
            result += scheduleDML + '\n';
        }
    }
    catch (err) {
        result += err.toString() + '\n';
        loggerStatus = 'FAILED';
        loggerMessage = err1.toString();
        var logStmnt = snowflake.createStatement ({ sqlText: loggerQuery, binds: [loggerStatus, loggerMessage, "CALL CTRL_TASK_SCHEDULER(...)"]});
        var logRsult = logStmnt.execute();
    }
    return result;
$$;



/********************************************************************
 ** Data Pipeline Solution Automation Setup
 ********************************************************************/
--
-- create a snow job to schedule data-load tasks;
--
/*
CREATE OR REPLACE TASK RUN_CTRL_SCHEMA_UPDATER
  WAREHOUSE = AIRFLOW_WH
  SCHEDULE = 'USING CRON 0/5 * * * * UTC'
AS
CALL CTRL_SCHEMA_UPDATER('WORK');

CREATE OR REPLACE TASK SCHEDULE_CTRL_DATA_LOADER
  WAREHOUSE = AIRFLOW_WH
  AFTER RUN_CTRL_SCHEMA_UPDATER
AS
CALL CTRL_TASK_SCHEDULER('DATA_LOADER','WORK');
*/

--
-- enable or disable the data-load task tree;
--
/*
--ALTER TASK _METADATA.RUN_CTRL_SCHEMA_UPDATER RESUME;
ALTER TASK _METADATA.RUN_CTRL_SCHEMA_UPDATER SUSPEND;
SELECT SYSTEM$TASK_DEPENDENTS_ENABLE( '_METADATA.RUN_CTRL_SCHEMA_UPDATER' );
*/


--
-- create a snow job to schedule data-version task;
--
/*
CREATE OR REPLACE TASK SCHEDULE_CTRL_DATA_VERSION
  WAREHOUSE = AIRFLOW_WH
  SCHEDULE = 'USING CRON 57 23 * * * UTC'
AS
CALL CTRL_TASK_SCHEDULER('DATA_VERSION','WORK');
*/


--
-- enable or disable the data-version task tree;
--
/*
ALTER TASK _METADATA.SCHEDULE_CTRL_DATA_VERSION RESUME;
ALTER TASK _METADATA.SCHEDULE_CTRL_DATA_VERSION SUSPEND;
*/

/*
SHOW TASKS;
select *
from table(information_schema.task_history())
WHERE DATABASE_NAME = CURRENT_DATABASE()
order by scheduled_time DESC;
*/
