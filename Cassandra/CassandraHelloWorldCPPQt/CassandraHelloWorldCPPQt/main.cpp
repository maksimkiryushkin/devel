#include <QtCore/QCoreApplication>
#include <QThread>
#include <QDebug>
#include <QTime>
#include "InsertThread.h"
#include "CassHandy.h"

int main(int argc, char *argv[]) {
	QCoreApplication a(argc, argv);
	
	// CONNECT TO CASSANDRA

	CassCluster* cluster;
	CassSession* session;
	if (!CassConnectToKeyspace("127.0.0.1", "chaos", "'class': 'SimpleStrategy', 'replication_factor': 1", &cluster, &session)) {
		// free resources
		CassFreeClusterAndSession(cluster, session);
		return 0;
	} else
		printf("connected to DB\n");

	// CREATE TABLE

	if (CassExecuteQuery(session,
		"CREATE TABLE IF NOT EXISTS test (\
			group int, \
			num bigint, \
			minimize bigint, \
			PRIMARY KEY (group, num)\
		)"
	) != CASS_OK) {
		// error has been printed within CassExecuteQuery
		// free resources
		CassFreeClusterAndSession(cluster, session);
		return 0;
	} else// if error ...
		printf("table test created\n");

	// INSERTING NUMBERS

	QList<InsertThread*> insertThreads;
	QTime elapsedTimer;
	elapsedTimer.start();

	InsertThread::insertNumCurrent = -1;
	InsertThread::insertNumMax = 100000 - 1;

	// creating some threads
	for (int k = 0; k < 40; ++k)
		insertThreads.push_back(new InsertThread());
	// go-go-go
	for (auto t : insertThreads)
		t->start();
	// waiting for all
	for (auto t : insertThreads)
		t->wait();

	printf("all threads are done\n");

	double elapsed = elapsedTimer.elapsed();
	qDebug() << "Elapsed " << elapsed << " msec for " << (InsertThread::insertNumMax + 1) << " inserts";
	qDebug() << "\t" << ((InsertThread::insertNumMax + 1) / (elapsed / 1000)) << " inserts per second";

	// free resources
	for (auto t : insertThreads)
		delete t;

	CassFreeClusterAndSession(cluster, session);

	//system("pause");
	return 0;
}
