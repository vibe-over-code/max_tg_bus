import asyncio

try:
    from pymax import Client, ExtraConfig
    MODERN_PYMAX = True
except ImportError:
    from pymax.core import SocketMaxClient as Client
    from pymax.payloads import UserAgentPayload
    MODERN_PYMAX = False

async def main():
    # Для входа по номеру телефона
    phone_number = str(input('phone: '))
    
    if MODERN_PYMAX:
        client = Client(
            phone=phone_number,
            work_dir="./data/cache",
            extra_config=ExtraConfig(reconnect=False, relogin=False),
        )
    else:
        client = Client(
            phone=phone_number,
            work_dir="./data/cache",
            headers=UserAgentPayload(device_type="DESKTOP"),
            reconnect=False,
        )

    # Запускаем клиента асинхронно
    await client.start()
    await client.close()
    
    # Теперь можно обращаться к токену
    token = getattr(client, "_token", None)
    if token:
        print(f"Token: {token}")
    else:
        print("Сессия сохранена в data/cache. Токен вручную копировать не нужно.")

# Запуск программы
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
