#include "CassandraCPP2.2.2-x86\cassandra.h"

CassError CassExecuteQuery(CassSession* session, const char* query);
void CassPrintError(CassFuture* future);
bool CassConnectToKeyspace(const char* contactPoints, const char* keyspace, const char* replicationParams, CassCluster** cluster, CassSession** session);
void CassFreeClusterAndSession(CassCluster* cluster, CassSession* session);
