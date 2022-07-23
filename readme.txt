Airflow服务器使用python+0mq发出调用请求，该程序监听到任务，执行命令行，成果返回给airflow 0，失败返回给airflow 其他值。使用request-response模式，只能一个个的顺序调用，不能并行运行。
