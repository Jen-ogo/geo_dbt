SELECT
  CURRENT_ORGANIZATION_NAME() AS org,
  CURRENT_ACCOUNT_NAME()      AS account,
  CURRENT_REGION()            AS region,
  CURRENT_VERSION()           AS version,
  CURRENT_USER()              AS user,
  CURRENT_ROLE()              AS role,
  CURRENT_WAREHOUSE()         AS wh,
  CURRENT_DATABASE()          AS db,
  CURRENT_SCHEMA()            AS schema