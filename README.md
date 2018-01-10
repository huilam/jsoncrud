# Introduction

A lightweight JSON JDBC ORM framework (39,508 bytes) that created to simplify RESTful API implementation as author tire of bloated opensource RDBMS ORM frameworks and its complicated dependencies. 

3rd party dependencies of jsoncrud are

1. JSON-java (https://github.com/stleary/JSON-java) - 48,224 bytes
2. database server JDBC drivers - 676,247 bytes



# Sample Java code

#### jsoncrud.properties
```
crud.jsoncrud_cfg.dbconfig=jdbc.postgres
crud.jsoncrud_cfg.tablename=jsoncrud_cfg
crud.jsoncrud_cfg.jsonattr.id.colname=cfg_id
crud.jsoncrud_cfg.jsonattr.appNamespace.colname=cfg_app_namespace
crud.jsoncrud_cfg.jsonattr.moduleCode.colname=cfg_module_code
crud.jsoncrud_cfg.jsonattr.createdTimestamp.colname=created_timestamp
crud.jsoncrud_cfg.jsonattr.enabled.colname=enabled
crud.jsoncrud_cfg.jsonattr.values.sql=select cfg_key, cfg_value from jsoncrud_cfg_values where cfg_id = {id} 

jdbc.postgres.classname=org.postgresql.Driver
jdbc.postgres.url=jdbc:postgresql://127.0.0.1:5432/postgres
jdbc.postgres.uid=postgres
jdbc.postgres.pwd=password
```

#### Initialize
```
CRUDMgr crudmgr = new CRUDMgr();
crudmgr.init();
```
      
#### Create
```
JSONObject jsonUser = new JSONObject(); 
jsonUser.put("appNamespace", "my-app"); 
jsonUser.put("moduleCode", "my-app-module1");
jsonUser.put("enabled", false);
jsonUser = crudmgr.create("crud.jsoncrud_cfg", jsonUser);
```

#### Retrieve
```
JSONObject jsonWhere = new JSONObject();
jsonWhere.put("appNamespace", "my-app");
jsonUser = crudmgr.retrieveFirst("crud.jsoncrud_cfg", jsonWhere);
```
  
#### Update
```
JSONObject jsonWhere = new JSONObject();
jsonWhere.put("appNamespace", "my-app");
jsonUser.put("moduleCode", "my-app-module1");

JSONObject jsonConfig = new JSONObject();
jsonConfig.put("enabled", true);
jsonConfig = crudmgr.update("crud.jsoncrud_cfg", jsonConfig, jsonWhere);
```

#### Delete
```
JSONObject jsonWhere = new JSONObject();
jsonWhere.put("appNamespace", "my-app");
jsonWhere.put("enabled", false);

jsonWhere = crudmgr.delete("crud.users", jsonWhere);
```

--
# JsonCrud Sample "test/hl/jsondao/CRUDMgrTest.java"
```
1. Download and setup postgres database server from https://www.postgresql.org/download/
2. Create sample application database schema by execute "test/create-test-schema.sql"
3. Download postgres JDBC driver from https://jdbc.postgresql.org/download.html 
4. Copy postgres jdbc driver jar to "lib" and add the jar file as project dependency 
4. Make sure "test/jsoncrud.properties" is in classpath
5. Execute "test/hl/jsondao/CRUDMgrTest" 
```