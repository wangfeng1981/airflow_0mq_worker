// 0mq程序，启动程序开始监听指定端口，airflow定时任务会将命令发给这个端口
// 使用C++开发要求跨平台，win，centos，ubuntu等
// 2022-6-20
#include <vector>
#include <string>
#include <iostream>
#include <zmq.h>
#include <string.h>
#include <stdio.h>

using namespace std;

int main(int argc , char* argv[] ) {
	cout<<"A airflow 0mq worker. 2022-6-20"<<endl ;
	cout<<"v0.1.0.2 first created."<<endl ;

	cout<<"Usage airflow_0mq_worker listening_port"<<endl ;

	if( argc!=2 ){
		cout<<"argc not 2, quit."<<endl ;
		return 11;
	}

	string port = argv[1] ;
	cout<<"will listening port of '"<<port<<"'"<<endl ;

	string tcpUrl = string("tcp://*:") + port ;
	cout<<"tcpUrl:"<<tcpUrl<<endl ;

	//  Socket to talk to clients
    void *context = zmq_ctx_new ();
    void *responder = zmq_socket (context, ZMQ_REP);
    int rc = zmq_bind (responder, tcpUrl.c_str() );
    if( rc!=0 ){
    	cout<<"zmq_bind failed "<<endl ;
    	return 12 ;
    }

    while (1) {
    	cout<<"------------------------"<<endl ;
    	cout<<"standby for receiving..."<<endl ;
        char buffer [2048];
        int nRecvBytes = zmq_recv (responder, buffer, 2048 , 0);
        buffer[nRecvBytes] = '\0' ;
        cout<<"recv cmd:"<<buffer<<endl ;
        buffer[2047] = '\0' ;
        cout<<"start running the cmd..."<<endl ;
        int res = system( buffer ) ;
        cout<<"cmd result:"<<res<<endl ;
       	sprintf(buffer,"%d", res) ;
       	cout<<"sending cmd result back to airflow..."<<endl ;
        zmq_send (responder, buffer , strlen(buffer) , 0);
        cout<<"sending done."<<endl ;
    }

	return 0 ;
}