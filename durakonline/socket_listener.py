
import socket
import socks
import json
import requests
import threading
import random
import atexit


class SocketListener:
    def __init__(self, client, proxy: str = ''):
        self.receive: list = []
        self.client = client
        self.proxy = proxy
        self.alive = False
        self.api_url: str = 'http://static.rstgames.com/durak/'
        self.socket = None
        self.handlers = {}
        self.thread = None

        atexit.register(self.shutdown)

    def create_connection(self):
        ip = self.ip
        port = self.port

        if not ip:
            server_data = self.get_servers()

            if not server_data or 'user' not in server_data:
                raise RuntimeError(
                    'Не удалось получить список игровых серверов.'
                )

            servers = server_data['user']

            # Диагностика списка серверов в логах Render.
            self.logger.info(
                f'{self.tag}: Получен список игровых серверов: {servers}'
            )

            if not servers:
                raise RuntimeError(
                    'Список игровых серверов пуст.'
                )

            if self.server_id:
                server = servers.get(self.server_id)

                if not server:
                    raise RuntimeError(
                        f'Сервер с ID {self.server_id} не найден.'
                    )
            else:
                server = random.choice(list(servers.values()))

            ip = server['host']
            port = server['port']

            self.logger.info(
                f'{self.tag}: Выбран игровой сервер {ip}:{port}'
            )

        if self.proxy:
            proxy_login, proxy_password, proxy_ip, proxy_port = (
                self.proxy.replace('@', ':').split(':')
            )

            socks.set_default_proxy(
                socks.SOCKS5,
                proxy_ip,
                int(proxy_port),
                username=proxy_login,
                password=proxy_password,
            )
            socket.socket = socks.socksocket

        self.socket = socket.socket(
            socket.AF_INET,
            socket.SOCK_STREAM,
        )

        try:
            self.socket.settimeout(10)
            self.socket.connect((ip, port))
        except Exception as e:
            if 'error' in self.handlers:
                self.handlers['error'](e)

            self.logger.error(
                f'{self.tag}: Ошибка подключения к {ip}:{port}: {e}'
            )

            try:
                self.socket.close()
            except Exception:
                pass

            self.socket = None
            return

        self.alive = True

        self.thread = threading.Thread(
            target=self.receive_messages,
        )
        self.thread.start()

    def send_server(self, data: dict):
        if not self.socket:
            raise ValueError('Socket not created')

        try:
            message_data = data.copy()
            command = message_data.pop('command')

            msg = (
                command
                + json.dumps(
                    message_data,
                    separators=(',', ':'),
                ).replace('{}', '')
                + '\n'
            ).encode()

            self.socket.sendall(msg)

        except Exception as e:
            if 'error' in self.handlers:
                self.handlers['error'](e)

    def get_servers(self):
        try:
            response = requests.get(
                f'{self.api_url}servers.json',
                timeout=10,
            )
            response.raise_for_status()
            return response.json()

        except Exception as e:
            if 'error' in self.handlers:
                self.handlers['error'](e)

            self.logger.error(
                f'{self.tag}: Не удалось получить список серверов: {e}'
            )
            return None

    def event(self, command: str = 'all'):
        def register_handler(handler):
            if command in self.handlers:
                self.handlers[command].append(handler)
            else:
                self.handlers[command] = [handler]
            return handler

        return register_handler

    def error(self):
        def register_handler(handler):
            self.handlers['error'] = handler
            return handler

        return register_handler

    def receive_messages(self):
        self.logger.debug(
            f'{self.tag}: Start listener'
        )

        _ = [
            handler()
            for handler in self.handlers.get('init', [])
        ]

        while self.alive:
            buffer = bytes()

            while self.alive:
                try:
                    r = self.socket.recv(4096)

                except Exception as e:
                    if not self.alive:
                        break

                    if 'error' in self.handlers:
                        self.handlers['error'](e)

                    self.logger.error(
                        f'{self.tag}: Ошибка чтения сокета: {e}'
                    )

                    self.alive = False
                    return

                if not r:
                    self.alive = False
                    break

                buffer += r

                try:
                    decoded = buffer.decode()
                except UnicodeDecodeError:
                    continue

                if not decoded.endswith('\n'):
                    continue

                buffer = bytes()

                for message_str in decoded.strip().split('\n'):
                    if not message_str:
                        continue

                    pos = message_str.find('{')

                    if pos < 0:
                        continue

                    command = message_str[:pos]

                    try:
                        message = json.loads(
                            message_str[pos:] + '}'
                        )
                    except Exception:
                        continue

                    message['command'] = command

                    self.logger.debug(
                        f'{self.tag}: {message}'
                    )

                    for handler_command in self.handlers:
                        if handler_command in ('all', command):
                            for handler in self.handlers[handler_command]:
                                handler(message)

                    self.receive.append(message)

        _ = [
            handler()
            for handler in self.handlers.get('shutdown', [])
        ]

    def listen(self, force: bool = False):
        while not self.receive:
            if force:
                return {'command': 'empty'}

        response = self.receive[0]
        del self.receive[0]

        return response

    def _get_data(self, command: str, force: bool = False):
        data = self.listen(force=force)

        while True:
            if data['command'] in (
                command,
                'err',
                'empty',
                'alert',
            ):
                return data

            data = self.listen(force=force)

    def shutdown(self) -> None:
        if self.alive:
            self.alive = False

        if self.socket is not None:
            try:
                self.socket.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

            try:
                self.socket.close()
            except OSError:
                pass

            self.socket = None

        if self.thread is not None:
            if self.thread is not threading.current_thread():
                self.thread.join(timeout=2)

            self.thread = None

    def __del__(self) -> None:
        atexit.unregister(self.shutdown)
