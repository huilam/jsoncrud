# Introduction

A lightweight JSON JDBC ORM framework that created to simplify RESTful API implementation as author tire of bloated opensource RDBMS ORM frameworks and its complicated dependencies. 

The only 3rd party dependency of jsoncrud is JSON-java (https://github.com/stleary/JSON-java), and your database server JDBC drivers.


# Sample Java code

#### jsoncrud.properties
```
crud.sample_users.dbconfig=jdbc.postgres
crud.sample_users.tablename=jsoncrud_sample_users
crud.sample_users.jsonattr.uid.colname=uid
crud.sample_users.jsonattr.displayname.colname=name
crud.sample_users.jsonattr.age.colname=age

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
jsonUser.put("uid", "my-uniqie-id"); 
jsonUser.put("displayname", "My Name");
jsonUser.put("age", 100);
jsonUser = crudmgr.create("crud.users", jsonUser);
```

#### Retrieve
```
JSONObject jsonWhere = new JSONObject();
jsonWhere.put("uid", "my-uniqie-id");
jsonUser = crudmgr.retrieveFirst("crud.users", jsonWhere);
```
  
#### Update
```
JSONObject jsonWhere = new JSONObject();
jsonWhere.put("uid", "my-uniqie-id");
JSONObject jsonUser = new JSONObject();
jsonUser.put("displayname", "My New Name");
jsonUser.put("age", 1);
jsonUser = crudmgr.update("crud.users", jsonUser, jsonWhere);
```

#### Delete
```
JSONObject jsonWhere = new JSONObject();
jsonWhere.put("uid", "my-uniqie-id");
jsonUser = crudmgr.delete("crud.users", jsonWhere);
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