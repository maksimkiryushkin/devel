#include "InsertThread.h"
#include "CassHandy.h"

// static vars from class
unsigned long InsertThread::insertNumCurrent;
unsigned long InsertThread::insertNumMax;
QMutex InsertThread::insertMutex;

bool InsertThread::GetNextNum() {
	QMutexLocker locker(&insertMutex);
	if ((insertNumCurrent != -1) && (insertNumCurrent >= insertNumMax))
		return false;
	num = ++insertNumCurrent;
	return true;
}

void InsertThread::run() {

	// CONNECT TO CASSANDRA

	CassCluster* cluster;
	CassSession* session;
	if (!CassConnectToKeyspace("127.0.0.1", "chaos", "'class': 'SimpleStrategy', 'replication_factor': 1", &cluster, &session)) {
		// free resources
		CassFreeClusterAndSession(cluster, session);
		return;
	}// if error ...

	// INSERTING NUMBERS

	const CassPrepared *preparedInsert = NULL;
	CassFuture* prefuture = cass_session_prepare(session, "INSERT INTO test (group, num, minimize) VALUES (?, ?, ?) IF NOT EXISTS");
	cass_future_wait(prefuture);
	CassError prerc = cass_future_error_code(prefuture);
	if (prerc != CASS_OK) {
		CassPrintError(prefuture);
		// free resources
		cass_future_free(prefuture);
		CassFreeClusterAndSession(cluster, session);
		return;
	} else
		preparedInsert = cass_future_get_prepared(prefuture);
	cass_future_free(prefuture);

	int group;
	unsigned long minimize;
	CassStatement* statement;
	CassFuture* future;
	CassError rcI;

	while (GetNextNum()) {
		group = num % 1000000;
		minimize = num;

		statement = cass_prepared_bind(preparedInsert);
		cass_statement_bind_int32(statement, 0, group);
		cass_statement_bind_int64(statement, 1, num);
		cass_statement_bind_int64(statement, 2, minimize);

		future = cass_session_execute(session, statement);
		cass_future_wait(future);
		rcI = cass_future_error_code(future);

		if (rcI != CASS_OK) {
			CassPrintError(future);
		}// if error ...

		cass_future_free(future);
		cass_statement_free(statement);
		
		// stop loop on error
		if (rcI != CASS_OK) break;

		// show progress on console
		if ((num % 3751) == 0) printf("%lu\n", num);
	}// foreach num ...

	// free resources
	cass_prepared_free(preparedInsert);
	CassFreeClusterAndSession(cluster, session);
}
