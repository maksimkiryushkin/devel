#include <stdio.h>
#include <string.h>
#include "CassHandy.h"

#pragma comment(lib, "CassandraCPP2.2.2-x86/cassandra.lib")

CassError CassExecuteQuery (CassSession* session, const char* query) {
	CassError rc = CASS_OK;
	CassFuture* future = NULL;
	CassStatement* statement = cass_statement_new(query, 0);
	future = cass_session_execute(session, statement);
	cass_future_wait(future);
	rc = cass_future_error_code(future);
	if (rc != CASS_OK) {
		CassPrintError(future);
	}// if error ...
	cass_future_free(future);
	cass_statement_free(statement);
	return rc;
}

void CassPrintError (CassFuture* future) {
	const char* message;
	size_t message_length;
	cass_future_error_message(future, &message, &message_length);
	fprintf(stderr, "Error: %.*s\n", (int)message_length, message);
}

bool CassConnectToKeyspace(const char* contactPoints, const char* keyspace, const char* replicationParams, CassCluster** cluster, CassSession** session) {
	*cluster = NULL;
	*session = NULL;

	// CONNECT
	// - setup cluster
	*cluster = cass_cluster_new();
	// - add contact points
	cass_cluster_set_contact_points(*cluster, contactPoints);
	// - connect to cluster
	*session = cass_session_new();
	// - provide the cluster object as configuration to connect the session
	CassFuture* connect_future = cass_session_connect(*session, *cluster);
	// - this operation will block until the result is ready
	CassError rc = cass_future_error_code(connect_future);
	if (rc != CASS_OK) {
		CassPrintError(connect_future);
		cass_future_free(connect_future);
		return false;
	}// if error ...
	cass_future_free(connect_future);

	// CREATE KEYSPACE
	char *query = new char[1000 + strlen(keyspace) + strlen(replicationParams)];
	sprintf(query, "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {%s}", keyspace, replicationParams);
	if (CassExecuteQuery(*session, query) != CASS_OK) {
		// error has been printed within CassExecuteQuery
		delete[] query;
		return false;
	}// if error ...
	delete[] query;

	// USE
	query = new char[10 + strlen(keyspace)];
	sprintf(query, "USE %s", keyspace);
	if (CassExecuteQuery(*session, query) != CASS_OK) {
		// error has been printed within CassExecuteQuery
		delete[] query;
		return false;
	}// if error ...
	delete[] query;

	return true;
}

void CassFreeClusterAndSession(CassCluster* cluster, CassSession* session) {
	cass_session_free(session);
	cass_cluster_free(cluster);
}
