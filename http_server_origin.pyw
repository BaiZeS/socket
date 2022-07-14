import http.server
import os, datetime


class RequestHandlerImpl(http.server.BaseHTTPRequestHandler):
    """
    自定义一个 HTTP 请求处理器
    """
    def do_GET(self):
        """
        处理 GET 请求, 处理其他请求需实现对应的 do_XXX() 方法
        """
        # 发送响应code
        self.send_response(200)
        # 发送响应头
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        # 发送响应内容（此处流不需要关闭）
        self.wfile.write("Hello World\n".encode("utf-8"))

    def do_POST(self):
        """
        处理 POST 请求
        """
        # 定义存储路径
        save_path = os.path.dirname(__file__)
        date_today = str(datetime.date.today())
        
        # 获取请求头
        addr = self.client_address        # 客户端地址和端口: (host, port)
        # 获取编码方式
        charset = self.headers['Content-Type'].split('=')[1]

        # 获取请求Body中的内容（需要指定读取长度, 不指定会阻塞）
        req_body = self.rfile.read(int(self.headers["Content-Length"])).decode(charset)

        # 将Body内容写入sqlserver

        # 保存数据
        file_name = os.path.join(save_path, 'data', addr[0] + '_' + date_today)
        with open(file_name, 'a+', encoding='utf-8') as f:
            f.write(req_body)
            f.write('\n'*2)
            f.close()

        # 发送响应code
        self.send_response(200)
        # 发送响应头
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        # 发送响应内容（此处流不需要关闭）
        self.wfile.write(('revice data length is %s \n' % len(req_body)).encode('utf-8'))



if __name__ == '__main__':
    # 定义服务器IP和端口
    server_host = '0.0.0.0'
    server_prot = 9001

    try:
        # 服务器绑定的地址和端口
        server_address = (server_host, server_prot)
        # 创建一个 HTTP 服务器（Web服务器）, 指定绑定的地址/端口 和 请求处理器
        httpd = http.server.HTTPServer(server_address, RequestHandlerImpl)
        # 循环等待客户端请求
        httpd.serve_forever()

    except Exception as e:
        er_log_path = os.path.join(os.path.dirname(__file__), 'error.log')
        with open(er_log_path, 'a+', encoding='utf-8') as ef:
            ef.write(repr(e))
            ef.write('\n'*2)
            ef.close()