#include <QtCore/QCoreApplication>
#include <QThread>

class InsertThread : public QThread {
    Q_OBJECT
public:
	static unsigned long insertNumCurrent;
	static unsigned long insertNumMax; // inclusive
	static QMutex insertMutex;
private:
	unsigned long num;
	bool GetNextNum();
public:
    void run();
};
