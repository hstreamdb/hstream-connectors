# source-postgresql

**Title:** source-postgresql

|                           |                                                                           |
| ------------------------- | ------------------------------------------------------------------------- |
| **Type**                  | `object`                                                                  |
| **Required**              | No                                                                        |
| **Additional properties** | [[Any type: allowed]](# "Additional Properties of any type are allowed.") |

**Description:** the configuration specification of source-postgresql

| Property                 | Pattern | Type    | Deprecated | Definition | Title/Description |
| ------------------------ | ------- | ------- | ---------- | ---------- | ----------------- |
| + [stream](#stream )     | No      | string  | No         | -          | stream name       |
| + [user](#user )         | No      | string  | No         | -          | User Name         |
| + [password](#password ) | No      | string  | No         | -          | Password          |
| + [host](#host )         | No      | string  | No         | -          | Hostname          |
| + [port](#port )         | No      | integer | No         | -          | Port Number       |
| + [database](#database ) | No      | string  | No         | -          | Database Name     |
| + [table](#table )       | No      | string  | No         | -          | Table Name        |

## <a name="stream"></a>1. Property `stream`

**Title:** stream name

|              |          |
| ------------ | -------- |
| **Type**     | `string` |
| **Required** | Yes      |

**Description:** the target HStream stream name

## <a name="user"></a>2. Property `user`

**Title:** User Name

|              |          |
| ------------ | -------- |
| **Type**     | `string` |
| **Required** | Yes      |

**Description:** postgresql user name

## <a name="password"></a>3. Property `password`

**Title:** Password

|              |          |
| ------------ | -------- |
| **Type**     | `string` |
| **Required** | Yes      |

**Description:** password of the user

## <a name="host"></a>4. Property `host`

**Title:** Hostname

|              |          |
| ------------ | -------- |
| **Type**     | `string` |
| **Required** | Yes      |

**Description:** postgresql hostname

**Example:** 

```json
"127.0.0.1"
```

## <a name="port"></a>5. Property `port`

**Title:** Port Number

|              |           |
| ------------ | --------- |
| **Type**     | `integer` |
| **Required** | Yes       |
| **Default**  | `5432`    |

**Description:** postgresql port number

**Example:** 

```json
5432
```

## <a name="database"></a>6. Property `database`

**Title:** Database Name

|              |          |
| ------------ | -------- |
| **Type**     | `string` |
| **Required** | Yes      |

**Description:** postgresql database name

## <a name="table"></a>7. Property `table`

**Title:** Table Name

|              |          |
| ------------ | -------- |
| **Type**     | `string` |
| **Required** | Yes      |

**Description:** table name of the database

**Examples:** 

```json
"t1"
```

```json
"public.t1"
```

----------------------------------------------------------------------------------------------------------------------------
