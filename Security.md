This file is just used to keep notes about the RBAC security implementation of KSQL.
It should be ignored and deleted once RBAC v1 is completely implemented.

What REST operations should be secured?
* "/"      RootDocument.java (@GET::get)
* "/query" StreamedQueryResource.java (@POST::streamQuery)
* "/ksql"  KsqlResource.java (@POST:handleKsqlStatements)
* "/ksql/terminate" KsqlResource.java (@POST:terminateCluster)
* "/info" ServerInfoResource.java (@POST::get)
* "/status" StatusResource.java (@GET::getAllStatuses)
* "/status/{entity}/{type}/{action}" StatusResource.java (@GET::get)

What actions should be allowed on REST operations?
* / READ
* /query CONNECT
* /ksql CONNECT
* /ksql/terminate TERMINATE
* /info READ
* /status READ
* /status/{entity}/{type}/{action} READ?
