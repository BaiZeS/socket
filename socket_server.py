import datetime
import socket
import threading, time
import os


save_path = os.path.dirname(__file__)
date_today = str(datetime.date.today())

def tcplink(sock, addr):
    print('Accept new connection from %s:%s...' % addr)
    sock.send(b'Welcome! please send "exit" to close this connect anytime you need~')      # str2byte
    while True:
        try:
            data = sock.recv(2048)
            time.sleep(1)

            if not data or data.decode('utf-8') == 'exit': 
                break

            sock.send(('received data length: %s' % str(len(data.decode('utf-8')))).encode('utf-8'))

            file_name = os.path.join(save_path, addr[0]+'_'+date_today)
            with open(file_name, 'a+', encoding='utf-8') as f:
                f.write(data.decode('utf-8'))
                f.write('\n'*2)
                f.close()
        except Exception as e:
            # er_log_path = os.path.join(os.path.dirname(__file__), 'error_message.log')
            # with open(er_log_path, 'a+', encoding='utf-8') as error_f:
            #     error_f.write('port %s get message error: %s \n' % (str(addr[1]), date_today))
            #     error_f.write(repr(e))
            #     error_f.write('\n'*3)
            #     error_f.close()
            pass

    sock.close()
    print('Connection from %s:%s closed.' % addr)



if __name__ == '__main__':
    # 定义服务器IP和端口
    server_host = '0.0.0.0'
    server_prot = 9001

    print('save path: %s' % save_path)

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    s.bind((server_host, server_prot))     # 监听端口:7001
    s.listen(5)     # 设定挂起数量为5
    print('Waiting for connection...')

    while True:
        try:
            sock, addr = s.accept()     # 接受一个新连接
            t = threading.Thread(target=tcplink, args=(sock, addr))     # 创建新线程来处理TCP连接
            t.start()
        except Exception as e:
            er_log_path = os.path.join(os.path.dirname(__file__), 'error.log')
            with open(er_log_path, 'a+', encoding='utf-8') as ef:
                ef.write(repr(e))
                ef.write('\n'*2)
                ef.close()
